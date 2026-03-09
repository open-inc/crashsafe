'use strict';
const fs = require('node:fs');
const path = require('node:path');
const zlib = require('node:zlib');
const { BSON: { EJSON } } = require('mongodb');
const { execFile } = require('node:child_process');
const { getDb, disconnect } = require('./db');
const { appendBackupEntry, readManifest } = require('./manifest');
const config = require('./config');
const logger = require('./logger');

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

/**
 * Load the ID snapshot from a previous backup run.
 * Returns { collectionName: Set<string> }
 * @param {string} dir
 * @param {string|null} idFile
 */
function loadIdSnapshot(dir, idFile) {
    if (!idFile) return {};
    const p = path.join(dir, 'ids', idFile);
    if (!fs.existsSync(p)) return {};
    const raw = JSON.parse(fs.readFileSync(p, 'utf-8'));
    // Deserialise arrays back to Sets
    return Object.fromEntries(Object.entries(raw).map(([k, v]) => [k, new Set(v)]));
}

/**
 * Save an ID snapshot for the current backup run.
 * @param {string} dir
 * @param {string} slug  Filename-safe identifier
 * @param {Record<string, Set<string>>} idSets
 * @returns {string} filename
 */
function saveIdSnapshot(dir, slug, idSets) {
    const idsDir = path.join(dir, 'ids');
    fs.mkdirSync(idsDir, { recursive: true });
    const filename = `${slug}.json`;
    const serialisable = Object.fromEntries(
        Object.entries(idSets).map(([k, v]) => [k, [...v]])
    );
    fs.writeFileSync(path.join(idsDir, filename), JSON.stringify(serialisable), 'utf-8');
    return filename;
}

/**
 * Execute mongodump via docker
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

    // The cutoff date: only documents with [updatedAtField] > lastRunDate are "changed"
    const lastRunDate = lastEntry ? idToDate(lastEntry.id) : null;

    // Load previous ID sets for delete detection
    const prevIdSets = loadIdSnapshot(dir, lastEntry?.idFile ?? null);

    const db = await getDb(dbName);
    const collectionInfos = await db.listCollections().toArray();
    let collectionNames = collectionInfos.map((c) => c.name);

    // Filter sensor collections in data DB
    if (dbType === 'data') {
        collectionNames = collectionNames.filter(
            (name) => name === config.sensorConfigCollection || name.startsWith(config.collectionPrefix)
        );
    }

    // Safe slug for filenames (colons and dots are not portable)
    const slug = id.replace(/[:.]/g, '-');                 // used in filenames

    const dumpOutDir = path.join(dir, slug);
    const trackingData = [];
    const newIdSets = {};
    const touchedCollections = [];

    for (const collName of collectionNames) {
        const isConfigColl = dbType === 'data' && collName === config.sensorConfigCollection;
        const coll = db.collection(collName);

        // --- Fetch changed/new documents ---
        const changedQuery = (isFull || isConfigColl || !lastRunDate)
            ? null
            : { [config.updatedAtField]: { $gt: lastRunDate } };

        // For mongodump, we only need to query if there has been an update, but we don't know
        // if docs changed without running the query. For incremental efficiency, we just let mongodump run.
        // It's fast if the query yields 0 results.
        
        // --- Fetch current full ID set (lightweight projection) for delete detection ---
        const currentIdDocs = await coll.find({}, { projection: { _id: 1 } }).toArray();
        const currentIdSet = new Set(currentIdDocs.map((d) => EJSON.stringify(d._id)));
        newIdSets[collName] = currentIdSet;

        // --- Detect deletes by comparing with previous ID set ---
        let deletedIds = [];
        let upsertedIds = [];
        if (!isFull && !isConfigColl) {
            const prevSet = prevIdSets[collName] ?? new Set();
            for (const prevId of prevSet) {
                if (!currentIdSet.has(prevId)) {
                    let typedId;
                    try { typedId = EJSON.parse(prevId); } catch (e) { typedId = prevId; }
                    deletedIds.push(typedId);
                }
            }
            
            // Collect IDs of documents that are changed, so restore.js can delete them before mongorestore inserts them
            if (changedQuery) {
                const upsertDocs = await coll.find(changedQuery, { projection: { _id: 1 } }).toArray();
                upsertedIds = upsertDocs.map(d => d._id);
            }
        }

        const hasChanges = (isFull || isConfigColl) 
            ? (await coll.countDocuments() > 0)
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
    }

    if (trackingData.length > 0) {
        fs.writeFileSync(path.join(dir, `${slug}.tracking.json`), EJSON.stringify(trackingData), 'utf-8');
    }

    const idFile = saveIdSnapshot(dir, slug, newIdSets);

    appendBackupEntry(dir, {
        id,          // ISO timestamp — used as the $gt cutoff for the next run
        type: isFull ? 'full' : 'incremental',
        dbType,
        collections: touchedCollections,
        file: fs.existsSync(dumpOutDir) ? slug : null, // Store directory name
        trackingFile: trackingData.length > 0 ? `${slug}.tracking.json` : null,
        idFile,
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
    const results = [];
    const id = new Date().toISOString();
    if ((!opts.target || opts.target === 'data' || opts.target === 'all') && config.dbData) {
        results.push(await backupDb(config.dbData, 'data', opts.full ?? false, id));
    }
    if ((!opts.target || opts.target === 'parse' || opts.target === 'all') && config.dbParse) {
        results.push(await backupDb(config.dbParse, 'parse', opts.full ?? false, id));
    }
    return results;
}

module.exports = { runBackup };
