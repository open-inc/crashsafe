'use strict';
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { runBackup } = require('./backup');
const { runRestore } = require('./restore');
const { readManifest } = require('./manifest');
const scheduler = require('./scheduler');
const config = require('./config');
const logger = require('./logger');

let server = null;

function getLatestBackups() {
    const dbs = [];
    if (config.dbData) dbs.push({ type: 'data', name: config.dbData });
    if (config.dbParse) dbs.push({ type: 'parse', name: config.dbParse });

    return dbs.map(db => {
        const dir = path.resolve(config.backupDir, db.type);
        const manifest = readManifest(dir);
        const last = manifest.backups.length ? manifest.backups[manifest.backups.length - 1] : null;
        return {
            ...db,
            lastBackup: last ? last.id : null,
            count: manifest.backups.length,
            history: manifest.backups.map(b => ({ id: b.id, type: b.type })).reverse()
        };
    });
}

const requestHandler = async (req, res) => {
    const { method, url } = req;

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
                runBackup({ full: isFull, target }).catch(err => logger.error({ err }, 'Manual backup failed'));
                
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
    const port = config.uiPort;
    server = http.createServer(requestHandler);
    server.listen(port, () => {
        logger.info({ port }, 'Status Page UI server started');
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
