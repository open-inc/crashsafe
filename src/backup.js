'use strict';
const fs = require('node:fs');
const path = require('node:path');
const readline = require('node:readline');
const { BSON: { EJSON } } = require('mongodb');
const { execFile } = require('node:child_process');
const { getDb, disconnect } = require('./db');
const { appendBackupEntry, readManifest, writeManifest, computeBackupSize } = require('./manifest');
const { computeBackupChecksums } = require('./checksum');
const { tryAcquireLock, releaseLock, updateLockProgress } = require('./locking');
const { redactErr, redactUri } = require('./uri-redact');
const config = require('./config');
const logger = require('./logger');

// ---------------------------------------------------------------------------
// Tunables
// ---------------------------------------------------------------------------

const COLLECTION_PAUSE_MS = 300;
const CURSOR_BATCH_SIZE = 1000;

// ---------------------------------------------------------------------------
// Concurrency control
// ---------------------------------------------------------------------------

// In-process mutex: prevents two backups from kicking off in the same Node
// process (e.g. cron + dashboard click). Cross-process protection lives in
// the lockfile via `tryAcquireLock` from ./locking.
let inFlight = null;

// In-memory tracking of the most recent run per trigger kind. Cleared on
// process restart — the persistent record lives in `manifest.json`, and
// `seedLastRuns` repopulates this on daemon start.
const lastRuns = {
    scheduled: null,
    manual: null,
};

function getRunStats() {
    return { ...lastRuns };
}

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
                // execFile's error object carries `cmd` (full argv including
                // --uri=mongodb://user:password@host) — must scrub before logging.
                logger.error({ error: redactErr(error), stderr: redactUri(stderr) }, 'mongodump failed');
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

async function backupDb(dbName, dbType, forceFull, id, trigger, appendOnly, dryRun = false) {
    const dir = dbBackupDir(dbType);
    fs.mkdirSync(dir, { recursive: true });

    const manifest = readManifest(dir);
    const lastEntry = manifest.backups.length ? manifest.backups[manifest.backups.length - 1] : null;
    const isFull = forceFull || !lastEntry;

    const lastRunDate = lastEntry ? idToDate(lastEntry.id) : null;
    // thisRunStartDate caps the changed-doc query so writes that happen DURING
    // this backup run aren't half-captured (collection A dumped early misses
    // them, collection B dumped late includes them — that produced
    // cross-collection inconsistency at restore-to-this-id). Doc.updatedAt
    // strictly less-than-or-equal to id ensures every collection sees the
    // exact same time window. Writes beyond this window flow into the next
    // inc, whose lower bound is `$gt: this.id`, so nothing is lost or
    // double-counted across the chain.
    const thisRunStartDate = idToDate(id);
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
    if (!dryRun) fs.mkdirSync(idsBaseDir, { recursive: true });

    const trackingData = [];
    const touchedCollections = [];
    // dry-run plan: collected per-collection so the caller can render or log
    // exactly what a real run would have done. Always populated; only
    // returned when dryRun is true.
    const plan = { id, dbType, type: isFull ? 'full' : 'incremental', collections: [] };

    let processedCount = 0;
    for (const collName of collectionNames) {
        updateLockProgress({
            currentDb: dbType,
            currentCollection: collName,
            processedCollections: processedCount,
            totalCollections: collectionNames.length,
        });
        const isConfigColl = dbType === 'data' && collName === config.sensorConfigCollection;
        const coll = db.collection(collName);

        // Append-only mode skips ID enumeration + delete detection — massively faster
        // for hot append-only streams (sensors), at the cost of not catching deletions.
        // The config collection is *always* exempt: small enough that full tracking is
        // free, and config-doc deletes are usually critical.
        const useAppendOnly = !!appendOnly && !isConfigColl;

        const changedQuery = (isFull || isConfigColl || !lastRunDate)
            ? null
            : { [config.updatedAtField]: { $gt: lastRunDate, $lte: thisRunStartDate } };

        let docCount = 0;
        const deletedIds = [];
        const upsertedIds = [];

        if (useAppendOnly) {
            // Fast path: no cursor over all _ids, no per-doc delete detection.
            // We still touch an empty marker JSONL so the *next* run can tell
            // whether this collection used to exist — append-only opts out of
            // per-document delete tracking, but collection-level drops are
            // always tracked (the comparison is just a readdir, no work per doc).
            if (!dryRun) fs.closeSync(fs.openSync(snapshotFile(dir, slug, collName), 'w'));
            if (isFull) {
                docCount = await coll.estimatedDocumentCount();
            } else if (changedQuery) {
                const upsertCursor = coll.find(changedQuery, { projection: { _id: 1 } }).batchSize(CURSOR_BATCH_SIZE);
                for await (const doc of upsertCursor) {
                    upsertedIds.push(doc._id);
                }
            }
        } else {
            // Standard path: full ID enumeration + delete detection.
            // For incrementals, both regular collections AND the config collection
            // need this — without it, a deleted doc silently re-appears on restore
            // (mongorestore skips duplicate _ids, so the prior chain entry's stale
            // doc persists).
            const needsDeleteDetection = !isFull;
            const currentIdSet = needsDeleteDetection ? new Set() : null;

            // In dry-run we still iterate the cursor (so delete detection
            // works against the prev run's JSONL on disk), but the write
            // target is a no-op stream — nothing lands on disk.
            const idStream = dryRun
                ? { write: () => true, end: (cb) => cb && cb(), once: () => {} }
                : fs.createWriteStream(snapshotFile(dir, slug, collName), { encoding: 'utf-8' });
            const idCursor = coll.find({}, { projection: { _id: 1 } }).batchSize(CURSOR_BATCH_SIZE);
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

            if (needsDeleteDetection) {
                for await (const prevIdStr of readPrevIds(dir, prevSlug, collName)) {
                    if (!currentIdSet.has(prevIdStr)) {
                        let typedId;
                        try { typedId = EJSON.parse(prevIdStr); } catch (e) { typedId = prevIdStr; }
                        deletedIds.push(typedId);
                    }
                }

                if (changedQuery) {
                    const upsertCursor = coll.find(changedQuery, { projection: { _id: 1 } }).batchSize(CURSOR_BATCH_SIZE);
                    for await (const doc of upsertCursor) {
                        upsertedIds.push(doc._id);
                    }
                } else if (isConfigColl) {
                    // Config collection has no updatedAt query (it's full-dumped every run).
                    // To make the chain replay idempotent — i.e. an incremental's dump
                    // truly overwrites the previous chain entry's config docs and not
                    // get silently skipped by mongorestore as duplicates — we mark every
                    // current _id as an "upsert" so replayEntry deletes the prior
                    // versions before mongorestore re-inserts the current truth.
                    for (const idStr of currentIdSet) {
                        let typedId;
                        try { typedId = EJSON.parse(idStr); } catch { continue; }
                        upsertedIds.push(typedId);
                    }
                }
            }

            if (currentIdSet) currentIdSet.clear();
        }

        const hasChanges = (isFull || isConfigColl)
            ? docCount > 0
            : upsertedIds.length > 0;

        if (hasChanges || deletedIds.length > 0) {
            touchedCollections.push(collName);
        }

        if (hasChanges && !dryRun) {
            await runMongoDump(config.uri, dbName, collName, changedQuery, dumpOutDir);
        }

        if (deletedIds.length > 0 || upsertedIds.length > 0) {
            trackingData.push({ op: 'track', collection: collName, deletes: deletedIds, upserts: upsertedIds });
        }

        plan.collections.push({
            name: collName,
            wouldDump: !!hasChanges,
            docCount,
            upserts: upsertedIds.length,
            deletes: deletedIds.length,
            mode: useAppendOnly ? 'append-only' : 'standard',
        });

        // Give MongoDB's WiredTiger cache a moment to evict and checkpoint.
        // No need to throttle in dry-run since we're not pressuring the cache.
        if (!dryRun) await sleep(COLLECTION_PAUSE_MS);
        processedCount++;
    }

    // Collection-drop detection: any collection that wrote a marker into the
    // previous run's id-snapshot but isn't present in this run's listCollections
    // was dropped between runs. Restore-replay needs an explicit drop op for it,
    // otherwise the prior chain entry's dump would silently re-create the
    // collection on restore. This runs in standard AND append-only mode — drops
    // are coarse-grained and the comparison is just a readdir.
    if (!isFull && prevSlug) {
        const prevSnapDir = snapshotDir(dir, prevSlug);
        if (fs.existsSync(prevSnapDir)) {
            const currentSet = new Set(collectionNames);
            for (const f of fs.readdirSync(prevSnapDir)) {
                if (!f.endsWith('.jsonl')) continue;
                const droppedColl = f.slice(0, -'.jsonl'.length);
                if (currentSet.has(droppedColl)) continue;
                // Honour the data-DB prefix filter so we never claim to "drop"
                // a collection we wouldn't have backed up in the first place.
                if (dbType === 'data' &&
                    droppedColl !== config.sensorConfigCollection &&
                    !droppedColl.startsWith(config.collectionPrefix)) continue;
                trackingData.push({ op: 'drop', collection: droppedColl });
                touchedCollections.push(droppedColl);
            }
        }
    }

    plan.trackingOps = trackingData.length;

    if (trackingData.length > 0 && !dryRun) {
        fs.writeFileSync(path.join(dir, `${slug}.tracking.json`), EJSON.stringify(trackingData), 'utf-8');
    }

    // Dry-run short-circuit: nothing was written, so there's nothing to size,
    // hash, or commit to the manifest. Return the plan; the caller decides
    // how to render it.
    if (dryRun) {
        plan.touchedCollections = touchedCollections;
        plan.dryRun = true;
        logger.info(
            { dbType, dbName, id, type: plan.type, collections: plan.collections.length, trackingOps: plan.trackingOps },
            'DRY RUN — no files written, no manifest update',
        );
        return plan;
    }

    // Size tracking is best-effort — a failure here must not prevent the manifest
    // entry from being written, otherwise the next run would miss this run's data
    // and re-do a full backup.
    let size = null;
    try {
        size = computeBackupSize(dir, slug);
    } catch (err) {
        logger.warn({ err, dbType, slug }, 'Failed to compute backup size; manifest entry will be written without it');
    }

    // Build the entry up-front so checksum computation can read the same shape
    // verify will. Checksums are best-effort for the same reason as size:
    // failing to hash should not break the chain. The next inc would be forced
    // back to a full if we aborted here.
    const entryDraft = {
        id,
        type: isFull ? 'full' : 'incremental',
        dbType,
        collections: touchedCollections,
        file: fs.existsSync(dumpOutDir) ? slug : null,
        trackingFile: trackingData.length > 0 ? `${slug}.tracking.json` : null,
        idDir: slug,
        size,
        trigger: trigger ?? 'unknown',
        finishedAt: new Date().toISOString(),
    };

    let checksums = null;
    try {
        checksums = await computeBackupChecksums(dir, entryDraft);
    } catch (err) {
        logger.warn({ err, dbType, slug }, 'Failed to compute backup checksums; manifest entry will be written without them');
    }

    appendBackupEntry(dir, { ...entryDraft, checksums });

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
    const triggerKind = (trigger === 'scheduled') ? 'scheduled' : 'manual';
    const dryRun = !!opts.dryRun;

    // Dry-run is purely a planning operation: it reads from MongoDB but writes
    // nothing (no JSONLs, no mongodump, no manifest update, no `lastRuns`
    // mutation). It also doesn't take the lock — concurrent operations might
    // shift the numbers slightly, which is acceptable for a preview.
    if (dryRun) {
        const id = new Date().toISOString();
        const results = [];
        if ((!opts.target || opts.target === 'data' || opts.target === 'all') && config.dbData) {
            results.push(await backupDb(config.dbData, 'data', opts.full ?? false, id, trigger, config.appendOnlyData, true));
        }
        if ((!opts.target || opts.target === 'parse' || opts.target === 'all') && config.dbParse) {
            results.push(await backupDb(config.dbParse, 'parse', opts.full ?? false, id, trigger, config.appendOnlyParse, true));
        }
        return { dryRun: true, results };
    }

    if (inFlight) {
        logger.warn({ trigger }, 'Backup skipped: another backup is already running in this process');
        return { skipped: true, reason: 'in-process' };
    }

    const acquired = tryAcquireLock('backup', trigger);
    if (!acquired.ok) {
        logger.warn({ trigger, holder: acquired.holder }, 'Backup skipped: another operation holds the lock');
        return { skipped: true, reason: 'cross-process', holder: acquired.holder };
    }

    const startedAt = new Date().toISOString();
    inFlight = (async () => {
        try {
            const results = [];
            const id = new Date().toISOString();
            if ((!opts.target || opts.target === 'data' || opts.target === 'all') && config.dbData) {
                results.push(await backupDb(config.dbData, 'data', opts.full ?? false, id, trigger, config.appendOnlyData));
            }
            if ((!opts.target || opts.target === 'parse' || opts.target === 'all') && config.dbParse) {
                results.push(await backupDb(config.dbParse, 'parse', opts.full ?? false, id, trigger, config.appendOnlyParse));
            }
            lastRuns[triggerKind] = {
                trigger,
                startedAt,
                finishedAt: new Date().toISOString(),
                status: 'success',
            };
            return results;
        } catch (err) {
            lastRuns[triggerKind] = {
                trigger,
                startedAt,
                finishedAt: new Date().toISOString(),
                status: 'error',
                error: err?.message ?? String(err),
            };
            throw err;
        } finally {
            releaseLock();
            inFlight = null;
        }
    })();

    return inFlight;
}

// ---------------------------------------------------------------------------
// Startup tasks — restore in-memory state from on-disk manifests
// ---------------------------------------------------------------------------

function configuredDbTypes() {
    const types = [];
    if (config.dbData) types.push('data');
    if (config.dbParse) types.push('parse');
    return types;
}

/**
 * One-shot pass that fills in `size` for any manifest entry that doesn't have
 * one yet (legacy entries from before size tracking, or interrupted runs that
 * left dump files but no size value). Safe to run on every daemon start; only
 * touches entries where `size` is missing.
 */
function backfillSizes() {
    for (const dbType of configuredDbTypes()) {
        const dir = dbBackupDir(dbType);
        let manifest;
        try {
            manifest = readManifest(dir);
        } catch (err) {
            logger.warn({ err, dbType }, 'Could not read manifest during backfill; skipping');
            continue;
        }

        let updated = 0;
        for (const entry of manifest.backups) {
            if (typeof entry.size === 'number') continue;
            try {
                const slug = entry.id.replace(/[:.]/g, '-');
                entry.size = computeBackupSize(dir, slug);
                updated++;
            } catch (err) {
                logger.warn({ err, id: entry.id }, 'Failed to backfill size for entry');
            }
        }

        if (updated > 0) {
            try {
                writeManifest(dir, manifest);
                logger.info({ dbType, updated }, 'Backfilled missing sizes in manifest');
            } catch (err) {
                logger.warn({ err, dbType }, 'Failed to write back manifest after backfill');
            }
        }
    }
}

/**
 * Seed the in-memory `lastRuns` from the most recent successful entries on
 * disk so the dashboard isn't empty after a daemon restart. Entries from older
 * versions without a `trigger` field are skipped — they'll be replaced by the
 * next run.
 */
function seedLastRuns() {
    const allEntries = [];
    for (const dbType of configuredDbTypes()) {
        try {
            const manifest = readManifest(dbBackupDir(dbType));
            allEntries.push(...manifest.backups);
        } catch { /* missing manifest is fine */ }
    }

    // Sort newest first by ISO id.
    allEntries.sort((a, b) => (a.id < b.id ? 1 : a.id > b.id ? -1 : 0));

    for (const entry of allEntries) {
        if (!entry.trigger) continue;
        const kind = (entry.trigger === 'scheduled') ? 'scheduled' : 'manual';
        if (!lastRuns[kind]) {
            lastRuns[kind] = {
                trigger: entry.trigger,
                startedAt: entry.id,
                finishedAt: entry.finishedAt ?? entry.id,
                status: 'success',
            };
        }
        if (lastRuns.scheduled && lastRuns.manual) break;
    }
}

// Orphan cleanup: paths that look like a slug (`<dbBackupDir>/<slug>` dump dir,
// `<dbBackupDir>/<slug>.tracking.json`, `<dbBackupDir>/ids/<slug>/`) but whose
// slug isn't recorded in any manifest entry. These accumulate when a backup
// crashes after writing some files but before `appendBackupEntry` runs — over
// months on a busy schedule that's significant disk waste, and confuses
// operators who `ls` the backup tree expecting one dir per manifest entry.

const SLUG_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-\d{3}Z$/;
const TRACKING_PATTERN = /^(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-\d{3}Z)\.tracking\.json$/;
// Don't touch anything younger than this — leaves a margin against the rare
// case of a CLI invocation racing daemon startup. With "always exactly one
// daemon" setups this is theoretical; the threshold is pure defense in depth.
const ORPHAN_AGE_THRESHOLD_MS = 60 * 60 * 1000;

/**
 * Convert a slug back to its underlying Date. Slugs are
 * `id.replace(/[:.]/g, '-')` of an ISO timestamp, so the inverse is just
 * putting the punctuation back at the right positions. Returns null on a
 * malformed slug — caller should treat that as "don't touch this dir".
 */
function slugToDate(slug) {
    const m = slug.match(/^(\d{4}-\d{2}-\d{2})T(\d{2})-(\d{2})-(\d{2})-(\d{3})(Z)$/);
    if (!m) return null;
    const iso = `${m[1]}T${m[2]}:${m[3]}:${m[4]}.${m[5]}${m[6]}`;
    const d = new Date(iso);
    return Number.isFinite(d.getTime()) ? d : null;
}

/**
 * Find and delete files left behind by crashed backups. Identifies any
 * slug-shaped path on disk that does NOT correspond to a manifest entry, and
 * is older than ORPHAN_AGE_THRESHOLD_MS, and removes it. Safe to call on
 * every daemon start.
 */
function cleanupOrphans() {
    const now = Date.now();

    for (const dbType of configuredDbTypes()) {
        const dir = dbBackupDir(dbType);
        if (!fs.existsSync(dir)) continue;

        let knownSlugs;
        try {
            const manifest = readManifest(dir);
            knownSlugs = new Set(
                manifest.backups
                    .map((b) => b.idDir || (b.id && b.id.replace(/[:.]/g, '-')))
                    .filter(Boolean)
            );
        } catch (err) {
            logger.warn({ err, dbType }, 'Could not read manifest during orphan cleanup; skipping');
            continue;
        }

        const removed = [];

        // Walk the top of <dbBackupDir>: dump dirs (slug-named directories) and
        // tracking files (`<slug>.tracking.json`). Anything else (manifest.json,
        // .backup.lock, the `ids/` dir itself) is left alone.
        let topEntries;
        try { topEntries = fs.readdirSync(dir, { withFileTypes: true }); }
        catch (err) {
            logger.warn({ err, dbType }, 'Could not read backup dir during orphan cleanup');
            continue;
        }

        for (const e of topEntries) {
            // Slug-named directory = orphan dump dir candidate
            if (e.isDirectory() && SLUG_PATTERN.test(e.name)) {
                if (knownSlugs.has(e.name)) continue;
                const slugDate = slugToDate(e.name);
                if (!slugDate || (now - slugDate.getTime()) < ORPHAN_AGE_THRESHOLD_MS) continue;
                const target = path.join(dir, e.name);
                try {
                    fs.rmSync(target, { recursive: true, force: true });
                    removed.push(e.name + '/');
                } catch (err) {
                    logger.warn({ err, target }, 'Failed to remove orphan dump dir');
                }
                continue;
            }

            // <slug>.tracking.json = orphan tracking file candidate
            if (e.isFile()) {
                const m = e.name.match(TRACKING_PATTERN);
                if (!m) continue;
                const slug = m[1];
                if (knownSlugs.has(slug)) continue;
                const slugDate = slugToDate(slug);
                if (!slugDate || (now - slugDate.getTime()) < ORPHAN_AGE_THRESHOLD_MS) continue;
                const target = path.join(dir, e.name);
                try {
                    fs.unlinkSync(target);
                    removed.push(e.name);
                } catch (err) {
                    logger.warn({ err, target }, 'Failed to remove orphan tracking file');
                }
            }
        }

        // ids/<slug>/ — orphan id-snapshot directory candidate
        const idsRoot = path.join(dir, 'ids');
        if (fs.existsSync(idsRoot)) {
            let idsEntries;
            try { idsEntries = fs.readdirSync(idsRoot, { withFileTypes: true }); }
            catch (err) {
                logger.warn({ err, dbType }, 'Could not read ids/ during orphan cleanup');
                idsEntries = [];
            }
            for (const e of idsEntries) {
                if (!e.isDirectory()) continue;
                if (!SLUG_PATTERN.test(e.name)) continue;
                if (knownSlugs.has(e.name)) continue;
                const slugDate = slugToDate(e.name);
                if (!slugDate || (now - slugDate.getTime()) < ORPHAN_AGE_THRESHOLD_MS) continue;
                const target = path.join(idsRoot, e.name);
                try {
                    fs.rmSync(target, { recursive: true, force: true });
                    removed.push('ids/' + e.name + '/');
                } catch (err) {
                    logger.warn({ err, target }, 'Failed to remove orphan ids dir');
                }
            }
        }

        if (removed.length > 0) {
            logger.info(
                { dbType, count: removed.length, sample: removed.slice(0, 5) },
                'Cleaned up orphan files from a previous crashed backup',
            );
        }
    }
}

/** Run all daemon-startup tasks. CLI invocations should NOT call this. */
function runStartupTasks() {
    backfillSizes();
    cleanupOrphans();
    seedLastRuns();
}

module.exports = { runBackup, getRunStats, runStartupTasks, cleanupOrphans };
