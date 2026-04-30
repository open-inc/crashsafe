'use strict';
const fs = require('node:fs');
const path = require('node:path');
const readline = require('node:readline');
const { BSON: { EJSON } } = require('mongodb');
const { execFile } = require('node:child_process');
const { getDb, disconnect } = require('./db');
const { appendBackupEntry, readManifest } = require('./manifest');
const config = require('./config');
const logger = require('./logger');

// ---------------------------------------------------------------------------
// Tunables
// ---------------------------------------------------------------------------

const COLLECTION_PAUSE_MS = 300;
const CURSOR_BATCH_SIZE = 1000;
const LOCKFILE_NAME = '.backup.lock';
const STALE_LOCK_THRESHOLD_MS = 24 * 60 * 60 * 1000;

// ---------------------------------------------------------------------------
// Concurrency control
// ---------------------------------------------------------------------------

let inFlight = null;
let heldLockPath = null;

function lockFilePath() {
    return path.resolve(config.backupDir, LOCKFILE_NAME);
}

function isProcessAlive(pid) {
    if (!pid || typeof pid !== 'number') return false;
    try {
        process.kill(pid, 0);
        return true;
    } catch {
        return false;
    }
}

/**
 * Try to atomically acquire the cross-process lockfile.
 * Returns { ok: true } on success, or { ok: false, holder } when another
 * live process holds it.
 */
function tryAcquireLock(trigger) {
    const lockPath = lockFilePath();
    fs.mkdirSync(path.dirname(lockPath), { recursive: true });

    if (fs.existsSync(lockPath)) {
        let lock = null;
        try {
            lock = JSON.parse(fs.readFileSync(lockPath, 'utf-8'));
        } catch {
            // Corrupted lockfile -> treat as stale
        }
        if (lock) {
            const startedAt = new Date(lock.startedAt).getTime();
            const ageMs = Number.isFinite(startedAt) ? Date.now() - startedAt : Infinity;
            const alive = isProcessAlive(lock.pid);
            if (alive && ageMs < STALE_LOCK_THRESHOLD_MS) {
                return { ok: false, holder: lock };
            }
            logger.warn({ stalePid: lock.pid, alive, ageMs }, 'Stale backup lock detected, reclaiming');
        }
        try { fs.unlinkSync(lockPath); } catch (e) {
            if (e.code !== 'ENOENT') throw e;
        }
    }

    const payload = JSON.stringify({
        pid: process.pid,
        startedAt: new Date().toISOString(),
        trigger,
    });
    try {
        // 'wx' = O_CREAT | O_EXCL: atomically fail if a concurrent acquirer wrote first
        fs.writeFileSync(lockPath, payload, { flag: 'wx', encoding: 'utf-8' });
    } catch (err) {
        if (err.code === 'EEXIST') {
            return { ok: false, holder: null };
        }
        throw err;
    }
    heldLockPath = lockPath;
    return { ok: true };
}

function releaseLock() {
    if (!heldLockPath) return;
    const p = heldLockPath;
    heldLockPath = null;
    try {
        fs.unlinkSync(p);
    } catch (err) {
        if (err.code !== 'ENOENT') {
            logger.warn({ err }, 'Failed to remove backup lock file');
        }
    }
}

// Synchronous safety net: if the process is exiting (clean shutdown or
// uncaught exception that bubbles out), drop the lockfile so it isn't
// stranded for the next process.
process.on('exit', () => {
    if (heldLockPath) {
        try { fs.unlinkSync(heldLockPath); } catch { /* ignore */ }
    }
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function dbBackupDir(dbType) {
    return path.resolve(config.backupDir, dbType);
}

/** Convert the stored ISO-timestamp ID back to a Date for MongoDB queries. */
function idToDate(id) {
    return new Date(id);
}

function snapshotDir(baseDir, slug) {
    return path.join(baseDir, 'ids', slug);
}

function snapshotFile(baseDir, slug, collName) {
    return path.join(snapshotDir(baseDir, slug), `${collName}.jsonl`);
}

/**
 * Stream EJSON-stringified _ids from a previous snapshot file, one per line.
 * Yields nothing if the file is missing (e.g. collection didn't exist last run).
 */
async function* readPrevIds(baseDir, slug, collName) {
    if (!slug) return;
    const filePath = snapshotFile(baseDir, slug, collName);
    if (!fs.existsSync(filePath)) return;
    const rl = readline.createInterface({
        input: fs.createReadStream(filePath, { encoding: 'utf-8' }),
        crlfDelay: Infinity,
    });
    for await (const line of rl) {
        if (line.length > 0) yield line;
    }
}

/** Write a line to a stream, awaiting drain if backpressure kicks in. */
async function writeLine(stream, line) {
    if (!stream.write(line + '\n')) {
        await new Promise((resolve) => stream.once('drain', resolve));
    }
}

function endStream(stream) {
    return new Promise((resolve, reject) => {
        stream.end((err) => (err ? reject(err) : resolve()));
    });
}

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Execute mongodump as a child process.
 */
function runMongoDump(uri, dbName, collName, query, outDir) {
    return new Promise((resolve, reject) => {
        const queryStr = query ? EJSON.stringify(query, { relaxed: false }) : '';
        const args = [
            `--uri=${uri}`,
            '--db', dbName,
            '--collection', collName,
            '--out', outDir,
            '--gzip'
        ];

        if (queryStr) {
            args.push('--query', queryStr);
        }

        execFile('mongodump', args, { maxBuffer: 1024 * 1024 * 10 }, (error, stdout, stderr) => {
            if (error) {
                logger.error({ error, stderr }, 'mongodump failed');
                reject(error);
            } else {
                resolve();
            }
        });
    });
}

// ---------------------------------------------------------------------------
// Core backup for a single database
// ---------------------------------------------------------------------------

async function backupDb(dbName, dbType, forceFull, id) {
    const dir = dbBackupDir(dbType);
    fs.mkdirSync(dir, { recursive: true });

    const manifest = readManifest(dir);
    const lastEntry = manifest.backups.length ? manifest.backups[manifest.backups.length - 1] : null;
    const isFull = forceFull || !lastEntry;

    const lastRunDate = lastEntry ? idToDate(lastEntry.id) : null;
    const prevSlug = lastEntry?.idDir ?? null;

    const db = await getDb(dbName);
    const collectionInfos = await db.listCollections().toArray();
    let collectionNames = collectionInfos.map((c) => c.name);

    if (dbType === 'data') {
        collectionNames = collectionNames.filter(
            (name) => name === config.sensorConfigCollection || name.startsWith(config.collectionPrefix)
        );
    }

    const slug = id.replace(/[:.]/g, '-');
    const dumpOutDir = path.join(dir, slug);
    const idsBaseDir = snapshotDir(dir, slug);
    fs.mkdirSync(idsBaseDir, { recursive: true });

    const trackingData = [];
    const touchedCollections = [];

    for (const collName of collectionNames) {
        const isConfigColl = dbType === 'data' && collName === config.sensorConfigCollection;
        const coll = db.collection(collName);

        const changedQuery = (isFull || isConfigColl || !lastRunDate)
            ? null
            : { [config.updatedAtField]: { $gt: lastRunDate } };

        // --- Stream current _ids to disk; only buffer in RAM if needed for delete detection ---
        const needsDeleteDetection = !isFull && !isConfigColl;
        const currentIdSet = needsDeleteDetection ? new Set() : null;

        const idStream = fs.createWriteStream(snapshotFile(dir, slug, collName), { encoding: 'utf-8' });
        const idCursor = coll.find({}, { projection: { _id: 1 } }).batchSize(CURSOR_BATCH_SIZE);
        let docCount = 0;
        try {
            for await (const doc of idCursor) {
                const idStr = EJSON.stringify(doc._id);
                if (currentIdSet) currentIdSet.add(idStr);
                await writeLine(idStream, idStr);
                docCount++;
            }
        } finally {
            await endStream(idStream);
        }

        // --- Detect deletes by streaming the previous run's snapshot ---
        const deletedIds = [];
        const upsertedIds = [];
        if (needsDeleteDetection) {
            for await (const prevIdStr of readPrevIds(dir, prevSlug, collName)) {
                if (!currentIdSet.has(prevIdStr)) {
                    let typedId;
                    try { typedId = EJSON.parse(prevIdStr); } catch (e) { typedId = prevIdStr; }
                    deletedIds.push(typedId);
                }
            }

            // Stream upserted _ids (no toArray, no full-collection load)
            if (changedQuery) {
                const upsertCursor = coll.find(changedQuery, { projection: { _id: 1 } }).batchSize(CURSOR_BATCH_SIZE);
                for await (const doc of upsertCursor) {
                    upsertedIds.push(doc._id);
                }
            }
        }

        // Release the per-collection Set before the next iteration
        if (currentIdSet) currentIdSet.clear();

        const hasChanges = (isFull || isConfigColl)
            ? docCount > 0
            : upsertedIds.length > 0;

        if (hasChanges || deletedIds.length > 0) {
            touchedCollections.push(collName);
        }

        if (hasChanges) {
            await runMongoDump(config.uri, dbName, collName, changedQuery, dumpOutDir);
        }

        if (deletedIds.length > 0 || upsertedIds.length > 0) {
            trackingData.push({ op: 'track', collection: collName, deletes: deletedIds, upserts: upsertedIds });
        }

        // Give MongoDB's WiredTiger cache a moment to evict and checkpoint
        await sleep(COLLECTION_PAUSE_MS);
    }

    if (trackingData.length > 0) {
        fs.writeFileSync(path.join(dir, `${slug}.tracking.json`), EJSON.stringify(trackingData), 'utf-8');
    }

    appendBackupEntry(dir, {
        id,
        type: isFull ? 'full' : 'incremental',
        dbType,
        collections: touchedCollections,
        file: fs.existsSync(dumpOutDir) ? slug : null,
        trackingFile: trackingData.length > 0 ? `${slug}.tracking.json` : null,
        idDir: slug,
    });

    logger.info(
        { dbType, dbName, id, type: isFull ? 'full' : 'incremental' },
        'Backup complete'
    );
    return { id, type: isFull ? 'full' : 'incremental' };
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

async function runBackup(opts = {}) {
    const trigger = opts.trigger ?? 'unknown';

    if (inFlight) {
        logger.warn({ trigger }, 'Backup skipped: another backup is already running in this process');
        return { skipped: true, reason: 'in-process' };
    }

    const acquired = tryAcquireLock(trigger);
    if (!acquired.ok) {
        logger.warn({ trigger, holder: acquired.holder }, 'Backup skipped: another process holds the lock');
        return { skipped: true, reason: 'cross-process', holder: acquired.holder };
    }

    inFlight = (async () => {
        try {
            const results = [];
            const id = new Date().toISOString();
            if ((!opts.target || opts.target === 'data' || opts.target === 'all') && config.dbData) {
                results.push(await backupDb(config.dbData, 'data', opts.full ?? false, id));
            }
            if ((!opts.target || opts.target === 'parse' || opts.target === 'all') && config.dbParse) {
                results.push(await backupDb(config.dbParse, 'parse', opts.full ?? false, id));
            }
            return results;
        } finally {
            releaseLock();
            inFlight = null;
        }
    })();

    return inFlight;
}

module.exports = { runBackup };
