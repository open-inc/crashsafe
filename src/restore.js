'use strict';
const fs = require('node:fs');
const path = require('node:path');
const { execFile } = require('node:child_process');
const { BSON: { EJSON } } = require('mongodb');
const { getDb, disconnect } = require('./db');
const { findEntry, getChainUpTo, getChainFrom } = require('./manifest');
const config = require('./config');
const logger = require('./logger');

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function dbBackupDir(dbType) {
    return path.resolve(config.backupDir, dbType);
}

/**
 * Execute mongorestore via native binary
 */
function runMongoRestore(uri, dbName, dumpDir) {
    return new Promise((resolve, reject) => {
        // mongorestore can take just the dumpDir and restores whatever is there mapping to the same db
        // To be safe we could pass --nsInclude="*.*", but mongorestore defaults to restoring everything.
        // We will just pass the dumpDir itself since mongodump places collections inside a folder named after dbName.
        const args = [
            `--uri=${uri}`,
            '--gzip',
            '--quiet',
            '--dir', dumpDir
        ];
        
        execFile('mongorestore', args, { maxBuffer: 1024 * 1024 * 10 }, (error, stdout, stderr) => {
            if (error) {
                logger.error({ error, stderr }, 'mongorestore failed');
                reject(error);
            } else {
                resolve();
            }
        });
    });
}

/**
 * Replay a backup entry (mongorestore + tracking deletions).
 * 
 * @param {object}   entry      Manifest entry
 * @param {string}   dir        Base backup directory
 * @param {object}   db         MongoDB Db instance
 */
async function replayEntry(entry, dir, db) {
    let totalUpserts = 0; // mongorestore doesn't easily expose upsert counts
    let totalDeletes = 0;

    // 1. Process deletions / upsert tracking
    // We explicitly delete updated documents BEFORE mongorestore inserts them, bypassing duplicate key errors
    if (entry.trackingFile) {
        const trackingPath = path.join(dir, entry.trackingFile);
        if (fs.existsSync(trackingPath)) {
            const trackingData = EJSON.parse(fs.readFileSync(trackingPath, 'utf8'));
            for (const trackOp of trackingData) {
                const coll = db.collection(trackOp.collection);
                const toDelete = [];
                if (trackOp.deletes?.length) toDelete.push(...trackOp.deletes);
                if (trackOp.upserts?.length) toDelete.push(...trackOp.upserts);
                if (toDelete.length > 0) {
                    await coll.deleteMany({ _id: { $in: toDelete } });
                    if (trackOp.deletes?.length) {
                        totalDeletes += trackOp.deletes.length;
                    }
                }
            }
        }
    }

    // 2. Process mongorestore
    if (entry.file) {
        const dumpDir = path.join(dir, entry.file);
        if (fs.existsSync(dumpDir)) {
            await runMongoRestore(config.uri, null, dumpDir);
        }
    }

    return { upserts: totalUpserts, deletes: totalDeletes };
}


// ---------------------------------------------------------------------------
// Incremental restore
// ---------------------------------------------------------------------------

/**
 * Apply a single backup file on top of the current database state.
 * Good for rolling forward from the current state.
 *
 * @param {'data'|'parse'} dbType
 * @param {string}         dbName
 * @param {string|null}    backupId  ISO ID or null for latest
 */
async function restoreIncremental(dbType, dbName, backupId) {
    const dir = dbBackupDir(dbType);
    const entry = findEntry(dir, backupId);
    if (!entry) {
        throw new Error(`Backup not found: ${backupId ?? '(latest)'} in ${dir}`);
    }

    logger.info({ dbType, dbName, backupId: entry.id }, 'Starting incremental restore');
    const db = await getDb(dbName);
    const stats = await replayEntry(entry, dir, db);

    logger.info({ ...stats, backupId: entry.id }, 'Incremental restore complete');
    return stats;
}

// ---------------------------------------------------------------------------
// Full restore
// ---------------------------------------------------------------------------

/**
 * Replay the full backup chain up to the given backupId.
 * Only drops existing collections when dropExisting is true.
 *
 * @param {'data'|'parse'} dbType
 * @param {string}         dbName
 * @param {string|null}    backupId     ISO ID or null for latest
 * @param {boolean}        dropExisting Drop all collections before restoring
 */
async function restoreFull(dbType, dbName, backupId, dropExisting) {
    const dir = dbBackupDir(dbType);
    const chain = getChainUpTo(dir, backupId ?? findEntry(dir, null)?.id);
    if (!chain.length) {
        throw new Error(`No backup chain found for ${dbType} up to ${backupId ?? '(latest)'}`);
    }

    logger.info({ dbType, dbName, steps: chain.length, dropExisting }, 'Starting full restore');

    const db = await getDb(dbName);

    // Drop all existing collections only when explicitly requested
    if (dropExisting) {
        const existing = await db.listCollections().toArray();
        for (const coll of existing) {
            await db.collection(coll.name).drop();
            logger.debug({ collection: coll.name }, 'Dropped collection');
        }
    }

    // Replay entire chain: first full, then each incremental in order
    let totalUpserts = 0;
    let totalDeletes = 0;

    for (const entry of chain) {
        logger.info({ backupId: entry.id, type: entry.type }, 'Replaying backup entry');
        const stats = await replayEntry(entry, dir, db);
        totalUpserts += stats.upserts;
        totalDeletes += stats.deletes;
    }

    logger.info({ upserts: totalUpserts, deletes: totalDeletes }, 'Full restore complete');
    return { upserts: totalUpserts, deletes: totalDeletes };
}

// ---------------------------------------------------------------------------
// Since restore
// ---------------------------------------------------------------------------

/**
 * Replay all backup files from sinceId (inclusive) up to backupId (or latest),
 * without dropping any collections. Useful to roll forward from a known-good point.
 *
 * @param {'data'|'parse'} dbType
 * @param {string}         dbName
 * @param {string}         sinceId   Start of the chain (inclusive)
 * @param {string|null}    toId      End of the chain (inclusive), or null for latest
 */
async function restoreSince(dbType, dbName, sinceId, toId) {
    const dir = dbBackupDir(dbType);
    const chain = getChainFrom(dir, sinceId, toId ?? null);
    if (!chain.length) {
        throw new Error(`No backups found for ${dbType} from ${sinceId} to ${toId ?? '(latest)'}`);
    }

    logger.info({ dbType, dbName, sinceId, toId: toId ?? '(latest)', steps: chain.length }, 'Starting since-restore');

    const db = await getDb(dbName);
    let totalUpserts = 0;
    let totalDeletes = 0;

    for (const entry of chain) {
        logger.info({ backupId: entry.id, type: entry.type }, 'Replaying backup entry');
        const stats = await replayEntry(entry, dir, db);
        totalUpserts += stats.upserts;
        totalDeletes += stats.deletes;
    }

    logger.info({ upserts: totalUpserts, deletes: totalDeletes }, 'Since-restore complete');
    return { upserts: totalUpserts, deletes: totalDeletes };
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * @param {'data'|'parse'|'all'} target   Which DB(s) to restore
 * @param {string|null}          backupId  Up-to target ID (null = latest)
 * @param {boolean}              full      Replay entire chain
 * @param {string|null}          sinceId   If set, replay chain from this ID without dropping
 * @param {boolean}              dropExisting  Drop collections before full restore
 */
async function runRestore(target, backupId, full, sinceId, dropExisting) {
    const targets = [];
    if ((target === 'data' || target === 'all') && config.dbData) {
        targets.push({ dbType: 'data', dbName: config.dbData });
    }
    if ((target === 'parse' || target === 'all') && config.dbParse) {
        targets.push({ dbType: 'parse', dbName: config.dbParse });
    }
    if (!targets.length) {
        throw new Error(`No configured database found for target "${target}"`);
    }

    for (const { dbType, dbName } of targets) {
        if (sinceId) {
            await restoreSince(dbType, dbName, sinceId, backupId ?? null);
        } else if (full) {
            await restoreFull(dbType, dbName, backupId, dropExisting);
        } else {
            await restoreIncremental(dbType, dbName, backupId);
        }
    }
}

module.exports = { runRestore };
