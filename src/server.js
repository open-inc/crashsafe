'use strict';
const http = require('node:http');
const https = require('node:https');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { runBackup, getRunStats } = require('./backup');
const { runRestore, getRestoreStats } = require('./restore');
const { runVerify, getVerifyStats } = require('./verify');
const { readManifest } = require('./manifest');
const { readLockInfo } = require('./locking');
const scheduler = require('./scheduler');
const config = require('./config');
const logger = require('./logger');
const { parseAllowList, isAllowed, getClientIp } = require('./ip-allow');
const {
    isSameOriginRequest,
    contentTypeBase,
    readBodyWithLimit,
    DEFAULT_MAX_BODY_BYTES,
} = require('./http-guards');
const { validateAccessGate, resolveTlsMode } = require('./startup-validation');

let server = null;
let allowList = []; // populated in start() from config.allowedIps

// ---------------------------------------------------------------------------
// Destructive-restore confirmation tokens
// ---------------------------------------------------------------------------
//
// A destructive restore (`dropExisting: true`) requires a fresh server-issued
// token, even at the API layer. This means a stray script that hits
// `/api/trigger/restore` with `dropExisting: true` cannot wipe the live DB
// in a single request — it must first fetch a token from
// `/api/restore/confirm`, then send it back. Tokens are single-use, expire
// after 60 seconds, and are bound to the target DB the operator confirmed.
// The dashboard's typed-`do it` modal still applies on top — UI friction
// AND server gate, complementary not redundant.

const CONFIRM_TTL_MS = 60_000;
const pendingConfirms = new Map(); // token -> { target, expiresAt }

function issueRestoreConfirm(target) {
    // Garbage-collect expired entries so the Map can't grow unbounded.
    const now = Date.now();
    for (const [k, v] of pendingConfirms) {
        if (v.expiresAt < now) pendingConfirms.delete(k);
    }
    const token = crypto.randomBytes(16).toString('hex');
    const expiresAt = now + CONFIRM_TTL_MS;
    pendingConfirms.set(token, { target, expiresAt });
    return { token, expiresAt: new Date(expiresAt).toISOString() };
}

function consumeRestoreConfirm(token, target) {
    if (typeof token !== 'string' || token.length === 0) return false;
    const entry = pendingConfirms.get(token);
    // Always delete on use, success or fail — single-shot semantics.
    pendingConfirms.delete(token);
    if (!entry) return false;
    if (entry.expiresAt < Date.now()) return false;
    if (entry.target !== target) return false;
    return true;
}

// ---------------------------------------------------------------------------
// IP allowlist (optional — empty list disables the gate)
// ---------------------------------------------------------------------------

/** Returns true if the request is from an allowed IP. On failure sends a 403 and returns false. */
function checkIpAllowed(req, res) {
    if (allowList.length === 0) return true;
    const ip = getClientIp(req, config.trustProxy);
    if (isAllowed(ip, allowList)) return true;

    logger.warn({ ip, trustProxy: config.trustProxy }, 'Dashboard request rejected — source IP not in allowlist');
    res.writeHead(403, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Forbidden');
    return false;
}

// ---------------------------------------------------------------------------
// CSRF / cross-origin gate (POST only)
// ---------------------------------------------------------------------------
//
// Without this gate, an attacker page could submit a same-credential POST
// (the browser auto-attaches Basic-Auth from cache) and trigger a backup or
// verify run. The destructive restore is already token-gated, but the other
// mutating endpoints relied solely on auth being present. We close that hole
// at the request layer by:
//
//   1. requiring `Content-Type: application/json` — eliminates the classic
//      `<form enctype="text/plain">` CSRF construction;
//   2. requiring `Origin` (or fallback `Referer`) to match the request's
//      `Host` — eliminates the cross-origin fetch attack with cached creds.

/** Returns true on same-origin JSON POSTs. On failure responds with 4xx and returns false. */
function checkCsrf(req, res) {
    if (req.method !== 'POST') return true;

    const ct = contentTypeBase(req);
    if (ct !== 'application/json') {
        logger.warn({ url: req.url, contentType: ct || '(missing)' }, 'POST rejected — Content-Type must be application/json');
        res.writeHead(415, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Content-Type must be application/json' }));
        return false;
    }

    if (!isSameOriginRequest(req)) {
        logger.warn({
            url: req.url,
            origin: req.headers.origin || null,
            referer: req.headers.referer || null,
            host: req.headers.host || null,
        }, 'POST rejected — Origin/Referer does not match Host (CSRF gate)');
        res.writeHead(403, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'cross-origin request rejected' }));
        return false;
    }
    return true;
}

// ---------------------------------------------------------------------------
// Body reader for POST handlers — wraps readBodyWithLimit and translates
// EBODYTOOLARGE into a 413 response.
// ---------------------------------------------------------------------------

async function readJsonBody(req, res) {
    try {
        return { ok: true, raw: await readBodyWithLimit(req, DEFAULT_MAX_BODY_BYTES) };
    } catch (err) {
        if (err && err.code === 'EBODYTOOLARGE') {
            logger.warn({ url: req.url, limit: DEFAULT_MAX_BODY_BYTES }, 'POST rejected — body exceeds size limit');
            res.writeHead(413, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: `request body exceeds ${DEFAULT_MAX_BODY_BYTES} bytes` }));
            return { ok: false };
        }
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'failed to read request body' }));
        return { ok: false };
    }
}

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


const requestHandler = async (req, res) => {
    const { method, url } = req;

    // Network-layer gate first: an unauthorized IP shouldn't even see the auth challenge.
    if (!checkIpAllowed(req, res)) return;
    if (!checkAuth(req, res)) return;
    // CSRF gate runs after auth so unauthorised origins still see 401, not 403 —
    // avoids leaking which is configured. POSTs only.
    if (!checkCsrf(req, res)) return;

    // Static files
    if (method === 'GET' && url === '/') {
        const filePath = path.join(__dirname, '..', 'public', 'index.html');
        if (fs.existsSync(filePath)) {
            // CSP is tuned for the Ant/React/Babel-on-CDN page. It's primarily
            // a clickjacking + form-CSRF gate (frame-ancestors / form-action);
            // 'unsafe-inline'/'unsafe-eval' are unavoidable because the page
            // uses inline <script type="text/babel"> and Babel's runtime
            // compiler. The page renders no untrusted input, so XSS surface
            // is effectively zero.
            res.writeHead(200, {
                'Content-Type': 'text/html; charset=utf-8',
                'X-Frame-Options': 'DENY',
                'X-Content-Type-Options': 'nosniff',
                'Referrer-Policy': 'no-referrer',
                'Content-Security-Policy': [
                    "default-src 'self'",
                    "script-src 'self' 'unsafe-inline' 'unsafe-eval' https://unpkg.com",
                    "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://unpkg.com",
                    "font-src 'self' https://fonts.gstatic.com",
                    "img-src 'self' data:",
                    "connect-src 'self'",
                    "frame-ancestors 'none'",
                    "base-uri 'self'",
                    "form-action 'self'",
                ].join('; '),
            });
            return fs.createReadStream(filePath).pipe(res);
        }
        res.writeHead(404);
        return res.end('Index not found');
    }

    // API: Status
    if (method === 'GET' && url === '/api/status') {
        const status = {
            scheduler: scheduler.getStatus(),
            verifyScheduler: scheduler.getVerifyStatus(),
            runs: getRunStats(),
            lastRestore: getRestoreStats(),
            lastVerify: getVerifyStats(),
            inFlight: readLockInfo(),
            backups: getLatestBackups(),
            config: {
                dbData: config.dbData,
                dbParse: config.dbParse,
                backupDir: config.backupDir,
                appendOnlyData: config.appendOnlyData,
                appendOnlyParse: config.appendOnlyParse
            }
        };
        res.writeHead(200, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify(status));
    }

    // API: Trigger Backup
    if (method === 'POST' && url === '/api/trigger/backup') {
        const body = await readJsonBody(req, res);
        if (!body.ok) return;
        try {
            const data = JSON.parse(body.raw || '{}');
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
        return;
    }

    // API: Issue a destructive-restore confirmation token.
    // Body: `{ target?: 'data' | 'parse' | 'all' }` (default 'all').
    // Response: `{ token, expiresAt }`. The token is required when calling
    // /api/trigger/restore with `dropExisting: true`. Tokens are single-use
    // and expire 60 s after issue. This makes a one-shot curl wipe attack
    // impossible: a caller must do two round-trips, and the second must
    // arrive within the window.
    if (method === 'POST' && url === '/api/restore/confirm') {
        const body = await readJsonBody(req, res);
        if (!body.ok) return;
        try {
            const data = JSON.parse(body.raw || '{}');
            const target = data.target || 'all';
            if (!['data', 'parse', 'all'].includes(target)) {
                res.writeHead(400, { 'Content-Type': 'application/json' });
                return res.end(JSON.stringify({ error: 'invalid target' }));
            }
            const issued = issueRestoreConfirm(target);
            logger.info({ target, expiresAt: issued.expiresAt }, 'Restore confirmation token issued');
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(issued));
        } catch {
            res.writeHead(400);
            res.end(JSON.stringify({ error: 'Invalid request' }));
        }
        return;
    }

    // API: Trigger Restore
    //
    // Body shape:
    //   {
    //     type:           'full' | 'incremental',
    //     target?:        'data' | 'parse' | 'all'   // default 'all'
    //     backupId?:      string                       // ISO id, null = latest
    //     sinceId?:       string                       // since-restore start
    //     dropExisting?:  boolean                      // explicit destruct flag
    //     mode?:          'direct' | 'sidecar'         // default 'direct'
    //     confirmToken?:  string                       // required for any destructive op
    //     verifyChecksums?: boolean                    // deep pre-flight
    //   }
    //
    // Destructive operations (`dropExisting: true` OR `mode: 'sidecar'`)
    // require a valid confirmToken from /api/restore/confirm; earlier
    // versions could be wiped with a single curl POST. Both UI typing and
    // server tokens apply.
    if (method === 'POST' && url === '/api/trigger/restore') {
        const body = await readJsonBody(req, res);
        if (!body.ok) return;
        try {
            const data = JSON.parse(body.raw || '{}');
            const isFull = data.type === 'full';
            const target = data.target || 'all';
            const sinceId = data.sinceId || null;
            const backupId = data.backupId || null;
            const dropExisting = !!data.dropExisting;
            const mode = data.mode === 'sidecar' ? 'sidecar' : 'direct';
            const verifyChecksums = !!data.verifyChecksums;

            if (data.mode && !['direct', 'sidecar'].includes(data.mode)) {
                res.writeHead(400, { 'Content-Type': 'application/json' });
                return res.end(JSON.stringify({ error: `invalid mode "${data.mode}" — expected "direct" or "sidecar"` }));
            }

            // Server-side gate: any destructive operation requires a
            // fresh, matching token from /api/restore/confirm. Sidecar
            // mode is destructive too — the swap replaces the live DB —
            // so the token gate applies regardless of `dropExisting`.
            const isDestructive = dropExisting || mode === 'sidecar';
            if (isDestructive) {
                const ok = consumeRestoreConfirm(data.confirmToken, target);
                if (!ok) {
                    logger.warn({ target, mode, hasToken: !!data.confirmToken }, 'Destructive restore rejected — invalid or missing confirm token');
                    res.writeHead(403, { 'Content-Type': 'application/json' });
                    return res.end(JSON.stringify({
                        error: 'destructive restore requires a confirm token',
                        hint: 'POST /api/restore/confirm with the same target first; tokens expire after 60 s and are single-use',
                    }));
                }
            }

            logger.info({ isFull, target, sinceId, backupId, dropExisting, mode, verifyChecksums }, 'Manual restore triggered via API');

            // Run in background
            runRestore(target, backupId, isFull, sinceId, dropExisting, 'api', { verifyChecksums, mode })
                .then((result) => {
                    if (result?.skipped) {
                        logger.warn({ reason: result.reason, holder: result.holder }, 'Manual restore skipped (another operation in progress)');
                    }
                })
                .catch(err => logger.error({ err }, 'Manual restore failed'));

            res.writeHead(202, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ message: 'Restore started' }));
        } catch (err) {
            res.writeHead(400);
            res.end(JSON.stringify({ error: 'Invalid request' }));
        }
        return;
    }

    // API: Trigger Verify
    // The body is { target?, backupId?, deep? }. Same async pattern as
    // /api/trigger/backup — 202 returns immediately, completion is observed
    // by polling /api/status (lastVerify + inFlight).
    if (method === 'POST' && url === '/api/trigger/verify') {
        const body = await readJsonBody(req, res);
        if (!body.ok) return;
        try {
            const data = JSON.parse(body.raw || '{}');
            const target = data.target || 'all';
            const backupId = data.backupId || null;
            const deep = !!data.deep;
            logger.info({ target, backupId, deep }, 'Manual verify triggered via API');

            runVerify({ target, backupId, deep, trigger: 'api' })
                .then((result) => {
                    if (result?.skipped) {
                        logger.warn({ reason: result.reason, holder: result.holder }, 'Manual verify skipped (another operation in progress)');
                    }
                })
                .catch(err => logger.error({ err }, 'Manual verify failed'));

            res.writeHead(202, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ message: 'Verify started' }));
        } catch (err) {
            res.writeHead(400);
            res.end(JSON.stringify({ error: 'Invalid request' }));
        }
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

    // Parse the IP allowlist eagerly so a malformed entry crashes the daemon
    // at startup with a clear error, instead of locking every legitimate
    // request out at runtime.
    try {
        allowList = parseAllowList(config.allowedIps);
    } catch (err) {
        throw new Error(`OPENINC_MONGO_BACKUP_ALLOWED_IPS is malformed: ${err.message}`);
    }

    // Mandatory: at least one of auth / IP allowlist.
    validateAccessGate({
        authOn: authEnabled(),
        ipAllowlistSize: allowList.length,
    });

    // Mandatory: HTTPS or explicit acknowledgement of plain HTTP.
    const tls = resolveTlsMode({
        tlsCert: config.tlsCert,
        tlsKey: config.tlsKey,
        allowInsecureHttp: config.allowInsecureHttp,
    });

    const port = config.uiPort;
    server = tls.mode === 'https'
        ? https.createServer({ key: tls.key, cert: tls.cert }, requestHandler)
        : http.createServer(requestHandler);

    server.listen(port, () => {
        logger.info({
            port,
            scheme: tls.mode,
            authEnabled: authEnabled(),
            ipAllowlistSize: allowList.length,
            trustProxy: config.trustProxy,
        }, 'Status Page UI server started');

        if (tls.mode === 'http') {
            logger.warn(
                'Daemon is listening on plain HTTP (ALLOW_INSECURE_HTTP=true). ' +
                'Only safe behind a TLS-terminating reverse proxy on the same trust boundary. ' +
                'Set OPENINC_MONGO_BACKUP_TLS_CERT and TLS_KEY to enable native HTTPS.'
            );
        }
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
