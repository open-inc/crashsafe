'use strict';
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { runBackup, getRunStats } = require('./backup');
const { runRestore } = require('./restore');
const { readManifest } = require('./manifest');
const scheduler = require('./scheduler');
const config = require('./config');
const logger = require('./logger');

const LOCKFILE_NAME = '.backup.lock';

let server = null;

// ---------------------------------------------------------------------------
// HTTP Basic Auth (optional — both env vars must be set to enable)
// ---------------------------------------------------------------------------

function authEnabled() {
    return Boolean(config.authUser) && Boolean(config.authPassword);
}

function timingSafeStringEquals(a, b) {
    const ab = Buffer.from(a, 'utf-8');
    const bb = Buffer.from(b, 'utf-8');
    if (ab.length !== bb.length) {
        // Run a dummy compare so the length-mismatch path takes similar time.
        crypto.timingSafeEqual(ab, ab);
        return false;
    }
    return crypto.timingSafeEqual(ab, bb);
}

function sendUnauthorized(res) {
    res.writeHead(401, {
        'WWW-Authenticate': 'Basic realm="CrashSafe", charset="UTF-8"',
        'Content-Type': 'text/plain; charset=utf-8',
    });
    res.end('Authentication required');
}

/** Returns true if the request is authorised (or auth is disabled). On failure sends a 401 and returns false. */
function checkAuth(req, res) {
    if (!authEnabled()) return true;

    const header = req.headers.authorization || '';
    if (!header.startsWith('Basic ')) {
        sendUnauthorized(res);
        return false;
    }

    let decoded;
    try {
        decoded = Buffer.from(header.slice(6).trim(), 'base64').toString('utf-8');
    } catch {
        sendUnauthorized(res);
        return false;
    }

    const sep = decoded.indexOf(':');
    if (sep === -1) {
        sendUnauthorized(res);
        return false;
    }
    const user = decoded.slice(0, sep);
    const pass = decoded.slice(sep + 1);

    const userOk = timingSafeStringEquals(user, config.authUser);
    const passOk = timingSafeStringEquals(pass, config.authPassword);
    if (!userOk || !passOk) {
        sendUnauthorized(res);
        return false;
    }
    return true;
}

function getLatestBackups() {
    const dbs = [];
    if (config.dbData) dbs.push({ type: 'data', name: config.dbData });
    if (config.dbParse) dbs.push({ type: 'parse', name: config.dbParse });

    return dbs.map(db => {
        const dir = path.resolve(config.backupDir, db.type);
        const manifest = readManifest(dir);
        const last = manifest.backups.length ? manifest.backups[manifest.backups.length - 1] : null;

        // Sum size from manifest entries — cheap and accurate for entries written
        // by this version. Old entries without `size` count as 0; they age out
        // naturally. Avoid walking the directory here since /api/status is polled
        // every few seconds and dirSize is O(files).
        const totalSize = manifest.backups.reduce((sum, b) => sum + (b.size || 0), 0);

        const history = manifest.backups.slice().reverse().map(b => ({
            id: b.id,
            type: b.type,
            size: typeof b.size === 'number' ? b.size : null,
            collections: Array.isArray(b.collections) ? b.collections.length : 0,
        }));

        return {
            ...db,
            lastBackup: last ? last.id : null,
            count: manifest.backups.length,
            totalSize,
            history,
        };
    });
}

function readLockInfo() {
    const lockPath = path.resolve(config.backupDir, LOCKFILE_NAME);
    if (!fs.existsSync(lockPath)) return null;
    try {
        return JSON.parse(fs.readFileSync(lockPath, 'utf-8'));
    } catch {
        return null;
    }
}

const requestHandler = async (req, res) => {
    const { method, url } = req;

    if (!checkAuth(req, res)) return;

    // Static files
    if (method === 'GET' && url === '/') {
        const filePath = path.join(__dirname, '..', 'public', 'index.html');
        if (fs.existsSync(filePath)) {
            res.writeHead(200, { 'Content-Type': 'text/html' });
            return fs.createReadStream(filePath).pipe(res);
        }
        res.writeHead(404);
        return res.end('Index not found');
    }

    // API: Status
    if (method === 'GET' && url === '/api/status') {
        const status = {
            scheduler: scheduler.getStatus(),
            runs: getRunStats(),
            inFlight: readLockInfo(),
            backups: getLatestBackups(),
            config: {
                dbData: config.dbData,
                dbParse: config.dbParse,
                backupDir: config.backupDir
            }
        };
        res.writeHead(200, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify(status));
    }

    // API: Trigger Backup
    if (method === 'POST' && url === '/api/trigger/backup') {
        let body = '';
        req.on('data', chunk => { body += chunk; });
        req.on('end', async () => {
            try {
                const data = JSON.parse(body || '{}');
                const isFull = data.type === 'full';
                const target = data.target || 'all';
                logger.info({ isFull, target }, 'Manual backup triggered via API');

                // Run in background to avoid blocking response
                runBackup({ full: isFull, target, trigger: 'api' })
                    .then((result) => {
                        if (result?.skipped) {
                            logger.warn({ reason: result.reason, holder: result.holder }, 'Manual backup skipped (another run in progress)');
                        }
                    })
                    .catch(err => logger.error({ err }, 'Manual backup failed'));

                res.writeHead(202, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ message: 'Backup started' }));
            } catch (err) {
                res.writeHead(400);
                res.end(JSON.stringify({ error: 'Invalid request' }));
            }
        });
        return;
    }

    // API: Trigger Restore
    if (method === 'POST' && url === '/api/trigger/restore') {
        let body = '';
        req.on('data', chunk => { body += chunk; });
        req.on('end', async () => {
            try {
                const data = JSON.parse(body || '{}');
                const isFull = data.type === 'full';
                const target = data.target || 'all';
                const sinceId = data.sinceId || null;
                logger.info({ isFull, target, sinceId }, 'Manual restore triggered via API');
                
                // Run in background
                runRestore(target, null, isFull, sinceId, isFull).catch(err => logger.error({ err }, 'Manual restore failed'));
                
                res.writeHead(202, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ message: 'Restore started' }));
            } catch (err) {
                res.writeHead(400);
                res.end(JSON.stringify({ error: 'Invalid request' }));
            }
        });
        return;
    }

    res.writeHead(404);
    res.end('Not Found');
};

function start() {
    // Reject half-configured auth (one var set without the other) so it can't
    // silently fall back to "no auth" in production.
    const userSet = Boolean(config.authUser);
    const passSet = Boolean(config.authPassword);
    if (userSet !== passSet) {
        throw new Error(
            'OPENINC_MONGO_BACKUP_AUTH_USER and OPENINC_MONGO_BACKUP_AUTH_PASSWORD must both be set or both unset.'
        );
    }

    const port = config.uiPort;
    server = http.createServer(requestHandler);
    server.listen(port, () => {
        logger.info({ port, authEnabled: authEnabled() }, 'Status Page UI server started');
    });
}

function stop() {
    if (server) {
        server.close();
        server = null;
        logger.info('Status Page UI server stopped');
    }
}

module.exports = { start, stop };
