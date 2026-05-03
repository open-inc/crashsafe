'use strict';
const fs = require('node:fs');
const path = require('node:path');
const { execFile } = require('node:child_process');
const { BSON: { EJSON } } = require('mongodb');
const { disconnect } = require('./db');
const { findEntry, getChainUpTo, getChainFrom } = require('./manifest');
const { tryAcquireLock, releaseLock, updateLockProgress } = require('./locking');
const { verifyEntry } = require('./verify');
const { safeJoin } = require('./checksum');
const { redactErr, redactUri } = require('./uri-redact');
const { getDb, getClient } = require('./db');
const config = require('./config');
const logger = require('./logger');

// In-process mutex + last-result tracking for restore. Mirrors the structure
// in backup.js so the dashboard can render a "Last Restore" stat the same way.
let inFlight = null;
let lastRestore = null;

function getRestoreStats() {
    return lastRestore;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function dbBackupDir(dbType) {
    return path.resolve(config.backupDir, dbType);
}

/**
 * Validate a restore chain against the on-disk state BEFORE any destructive
 * action runs. Catches the most common ways a chain has gone bad: a dump dir
 * was manually deleted, a tracking file got truncated, an entry's checksums
 * no longer match the file on disk.
 *
 * Always (cheap):
 *   - For each entry with a `file`, the dump dir exists.
 *   - For each entry with a `trackingFile`, the file exists and parses as EJSON.
 *
 * Optional (`opts.verifyChecksums`, slow on large chains):
 *   - For each entry with stored checksums, re-hash files on disk and compare.
 *
 * Throws a single Error aggregating every issue found across the chain. The
 * caller is responsible for treating that throw as "abort before destruction".
 *
 * Note: legacy entries without `checksums` are NOT a failure even with
 * `verifyChecksums: true` — they're a "no baseline" warning, logged but not
 * fatal. Same compromise as in the verify CLI (exit 2, not 1).
 */
async function preflightChain(dbType, dir, chain, opts = {}) {
    const issues = [];
    let processed = 0;

    for (const entry of chain) {
        updateLockProgress({
            currentDb: dbType,
            phase: 'preflight',
            currentEntry: entry.id,
            currentEntryType: entry.type,
            processedSteps: processed,
            totalSteps: chain.length,
        });

        // safeJoin: a tampered manifest with `../`-style paths must not even
        // get to the existsSync check, let alone the read.
        if (entry.file) {
            let dumpDir;
            try { dumpDir = safeJoin(dir, entry.file); }
            catch (err) {
                issues.push(`${entry.id}: rejected unsafe dump path "${entry.file}": ${err.message}`);
                processed++;
                continue;
            }
            if (!fs.existsSync(dumpDir)) {
                issues.push(`${entry.id}: dump dir missing (${entry.file})`);
            }
        }

        if (entry.trackingFile) {
            let trackPath;
            try { trackPath = safeJoin(dir, entry.trackingFile); }
            catch (err) {
                issues.push(`${entry.id}: rejected unsafe tracking path "${entry.trackingFile}": ${err.message}`);
                processed++;
                continue;
            }
            if (!fs.existsSync(trackPath)) {
                issues.push(`${entry.id}: tracking file missing (${entry.trackingFile})`);
            } else {
                try {
                    EJSON.parse(fs.readFileSync(trackPath, 'utf8'));
                } catch (err) {
                    issues.push(`${entry.id}: tracking file corrupt: ${err.message}`);
                }
            }
        }

        if (opts.verifyChecksums && entry.checksums) {
            const result = await verifyEntry(dbType, dir, entry, { deep: false });
            if (result.status === 'corrupt') {
                const sample = result.issues.slice(0, 3)
                    .map((i) => `${i.kind}/${i.path}=${i.status}`)
                    .join(', ');
                const more = result.issues.length > 3 ? `, +${result.issues.length - 3} more` : '';
                issues.push(`${entry.id}: ${result.issues.length} hash issue(s) — ${sample}${more}`);
            }
        }

        processed++;
    }

    if (issues.length) {
        const head = `Pre-flight check failed for ${dbType} restore chain (${issues.length} issue${issues.length === 1 ? '' : 's'}). Refusing to run the restore.`;
        const body = issues.map((s) => '  - ' + s).join('\n');
        throw new Error(head + '\n' + body);
    }
}

/**
 * Execute mongorestore via native binary.
 *
 * @param {string}   uri      MongoDB URI to restore into.
 * @param {string|null} _dbName  Unused (mongorestore reads the DB name from the
 *                                dump-dir layout).
 * @param {string}   dumpDir  Path to the dump root.
 * @param {{from: string, to: string} | null} rename
 *   Optional namespace rename. When set, mongorestore is invoked with
 *   `--nsFrom='<from>.*' --nsTo='<to>.*' --nsInclude='<from>.*'`, redirecting
 *   every namespace from the dump's source DB to a sidecar DB. Used by the
 *   sidecar-mode restore so the live database is never written to during
 *   replay.
 */
function runMongoRestore(uri, _dbName, dumpDir, rename = null) {
    return new Promise((resolve, reject) => {
        const args = [
            `--uri=${uri}`,
            '--gzip',
            '--quiet',
            '--dir', dumpDir,
        ];
        if (rename) {
            args.push('--nsInclude', `${rename.from}.*`);
            args.push('--nsFrom',    `${rename.from}.*`);
            args.push('--nsTo',      `${rename.to}.*`);
        }

        execFile('mongorestore', args, { maxBuffer: 1024 * 1024 * 10 }, (error, stdout, stderr) => {
            if (error) {
                // execFile's error object carries `cmd` (full argv including
                // --uri=mongodb://user:password@host) — must scrub before logging.
                logger.error({ error: redactErr(error), stderr: redactUri(stderr) }, 'mongorestore failed');
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
 * @param {object}   db         MongoDB Db instance — the destination of every
 *                              tracking-side write (deleteMany, drop). For
 *                              sidecar mode, this is the sidecar DB handle.
 * @param {object}   [opts]
 * @param {{from: string, to: string} | null} [opts.nsRename]
 *   Forwarded to runMongoRestore. When set, mongorestore will rewrite every
 *   namespace from `<from>.*` to `<to>.*` so the dump lands in the sidecar DB.
 *   `db` should be the sidecar handle whose name matches `opts.nsRename.to`.
 */
async function replayEntry(entry, dir, db, opts = {}) {
    let totalUpserts = 0; // mongorestore doesn't easily expose upsert counts
    let totalDeletes = 0;

    // 1. Process deletions / upsert tracking
    // We explicitly delete updated documents BEFORE mongorestore inserts them, bypassing duplicate key errors.
    // safeJoin ensures a tampered manifest can't escape the backup tree — refuse
    // to read a tracking file at e.g. `../../etc/passwd`.
    if (entry.trackingFile) {
        const trackingPath = safeJoin(dir, entry.trackingFile);
        if (fs.existsSync(trackingPath)) {
            const trackingData = EJSON.parse(fs.readFileSync(trackingPath, 'utf8'));
            for (const trackOp of trackingData) {
                // Collection-level drop: drop the whole collection. NamespaceNotFound
                // is tolerated because the collection may not exist yet at this point
                // in a PITR replay (e.g. it was dropped before being recreated).
                if (trackOp.op === 'drop') {
                    try {
                        await db.collection(trackOp.collection).drop();
                        totalDeletes++;
                    } catch (err) {
                        if (err?.codeName !== 'NamespaceNotFound') throw err;
                    }
                    continue;
                }
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

    // 2. Process mongorestore. safeJoin guards against a tampered manifest
    // pointing at a directory outside the backup tree. URI is config.restoreUri
    // so a separate restore destination (sandbox cluster) is honoured if set;
    // it falls back to config.uri when RESTORE_URI is unset. nsRename lets
    // sidecar-mode redirect the dump's namespace from `<liveDb>.*` to the
    // sidecar DB without touching the live database.
    if (entry.file) {
        const dumpDir = safeJoin(dir, entry.file);
        if (fs.existsSync(dumpDir)) {
            await runMongoRestore(config.restoreUri, null, dumpDir, opts.nsRename ?? null);
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
async function restoreIncremental(dbType, dbName, backupId, opts = {}) {
    const dir = dbBackupDir(dbType);
    const entry = findEntry(dir, backupId);
    if (!entry) {
        throw new Error(`Backup not found: ${backupId ?? '(latest)'} in ${dir}`);
    }

    // Pre-flight: never touch the live DB until we've confirmed the entry's
    // files are intact. Cheap for a single entry.
    await preflightChain(dbType, dir, [entry], opts);

    logger.info({ dbType, dbName, backupId: entry.id }, 'Starting incremental restore');
    updateLockProgress({
        currentDb: dbType,
        phase: 'replaying',
        currentEntry: entry.id,
        currentEntryType: entry.type,
        processedSteps: 0,
        totalSteps: 1,
    });
    // Restore destination respects RESTORE_URI when set; falls back to the
    // backup URI otherwise. All deleteMany / drop operations during the
    // restore go to the same destination as the mongorestore that follows.
    const db = await getDb(dbName, config.restoreUri);
    const stats = await replayEntry(entry, dir, db);
    updateLockProgress({
        currentDb: dbType,
        phase: 'done',
        processedSteps: 1,
        totalSteps: 1,
    });

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
async function restoreFull(dbType, dbName, backupId, dropExisting, opts = {}) {
    const dir = dbBackupDir(dbType);
    const chain = getChainUpTo(dir, backupId ?? findEntry(dir, null)?.id);
    if (!chain.length) {
        throw new Error(`No backup chain found for ${dbType} up to ${backupId ?? '(latest)'}`);
    }

    logger.info({ dbType, dbName, steps: chain.length, dropExisting }, 'Starting full restore');

    // Pre-flight check: validate the entire chain BEFORE we drop anything.
    // This is the critical guard — without it, a missing dump file or corrupt
    // tracking file is only discovered after the live DB has already been
    // wiped, leaving us with a half-restored database and no rollback.
    await preflightChain(dbType, dir, chain, opts);

    // Restore destination respects RESTORE_URI when set; falls back to the
    // backup URI otherwise. All deleteMany / drop operations during the
    // restore go to the same destination as the mongorestore that follows.
    const db = await getDb(dbName, config.restoreUri);

    // Drop all existing collections only when explicitly requested
    if (dropExisting) {
        updateLockProgress({ currentDb: dbType, phase: 'dropping', totalSteps: chain.length, processedSteps: 0 });
        const existing = await db.listCollections().toArray();
        for (const coll of existing) {
            await db.collection(coll.name).drop();
            logger.debug({ collection: coll.name }, 'Dropped collection');
        }
    }

    // Replay entire chain: first full, then each incremental in order
    let totalUpserts = 0;
    let totalDeletes = 0;

    let step = 0;
    for (const entry of chain) {
        updateLockProgress({
            currentDb: dbType,
            phase: 'replaying',
            currentEntry: entry.id,
            currentEntryType: entry.type,
            processedSteps: step,
            totalSteps: chain.length,
        });
        logger.info({ backupId: entry.id, type: entry.type }, 'Replaying backup entry');
        const stats = await replayEntry(entry, dir, db);
        totalUpserts += stats.upserts;
        totalDeletes += stats.deletes;
        step++;
    }
    updateLockProgress({ currentDb: dbType, phase: 'done', processedSteps: chain.length, totalSteps: chain.length });

    logger.info({ upserts: totalUpserts, deletes: totalDeletes }, 'Full restore complete');
    return { upserts: totalUpserts, deletes: totalDeletes };
}

// ---------------------------------------------------------------------------
// Sidecar restore — replay into shadow DB, atomic per-collection swap
// ---------------------------------------------------------------------------
//
// Direct mode (above) drops the live DB first, then replays into it. If the
// replay fails midway (network drop, OOM, mongorestore bug at entry N), the
// live DB is in a half-restored state with no rollback.
//
// Sidecar mode replays the entire chain into `<dbName>__crashsafe_restore_<id>`
// instead. The live DB is untouched until the replay completes successfully.
// The final swap renames each sidecar collection over its live counterpart in
// a single mongo command per coll (`renameCollection ... dropTarget: true`),
// which is atomic from MongoDB's view. A swap-phase failure leaves a partial
// state — operator-visible, not silent — which is acceptable because the
// swap window is brief (rename is a metadata-only op, no data movement on
// the same shard).
//
// Sidecars from previous failed runs are cleaned up at start. The sidecar
// DB name is timestamp-based, so two concurrent sidecars never collide.

const SIDECAR_PREFIX = '__crashsafe_restore_';

/** Compute the sidecar DB name for a given live DB and run id. */
function sidecarDbName(liveDbName, runId) {
    return `${liveDbName}${SIDECAR_PREFIX}${runId.replace(/[:.]/g, '-')}`;
}

/**
 * Drop any sidecar databases left behind by previous failed restores.
 * Identified by the `<liveDbName>__crashsafe_restore_*` prefix. Logs a
 * warning per cleaned-up DB so the operator knows recovery happened.
 */
async function cleanupOrphanSidecars(client, liveDbName) {
    const result = await client.db('admin').admin().listDatabases();
    const orphans = result.databases
        .map((d) => d.name)
        .filter((name) => name.startsWith(`${liveDbName}${SIDECAR_PREFIX}`));
    for (const orphan of orphans) {
        logger.warn({ orphan, liveDbName }, 'Found orphan sidecar DB from a previous failed restore — dropping');
        try {
            await client.db(orphan).dropDatabase();
        } catch (err) {
            logger.warn({ err: redactErr(err), orphan }, 'Failed to drop orphan sidecar DB; manual cleanup required');
        }
    }
}

/**
 * Atomically swap each collection in `sidecarDb` to `liveDb`. Per-collection
 * `renameCollection ... dropTarget: true` is a single mongo command and is
 * atomic. If the swap fails midway, the operator sees a clear error showing
 * which collections were already swapped vs. still in the sidecar; the data
 * is still recoverable from the sidecar.
 *
 * Collections in live but not in sidecar:
 *   - If the chain recorded the collection as dropped → also drop it from live
 *   - Otherwise → leave it alone. This is intentional: collections in live
 *     that were never backed up (e.g. a non-prefixed audit_log in the data DB)
 *     would otherwise be lost. Sidecar mode is strictly *less* destructive
 *     than direct mode in this dimension.
 */
async function swapSidecarToLive(client, sidecarName, liveName, droppedCollections) {
    const sidecarDb = client.db(sidecarName);
    const liveDb = client.db(liveName);
    const sidecarColls = (await sidecarDb.listCollections().toArray()).map((c) => c.name);
    const liveColls = (await liveDb.listCollections().toArray()).map((c) => c.name);

    let swapped = 0;
    for (const collName of sidecarColls) {
        try {
            await client.db('admin').command({
                renameCollection: `${sidecarName}.${collName}`,
                to: `${liveName}.${collName}`,
                dropTarget: true,
            });
            swapped++;
        } catch (err) {
            const remaining = sidecarColls.slice(swapped + 1);
            logger.error(
                { err: redactErr(err), failedAt: collName, swapped, remaining },
                'Sidecar swap FAILED mid-flight. Live DB has a mix of new and old collections. ' +
                'Sidecar still holds the unswapped collections — finish the swap manually with ' +
                'db.adminCommand({renameCollection, to, dropTarget: true}) or drop the sidecar to ' +
                'roll back to a fully-old state.'
            );
            throw err;
        }
    }

    // Collections in live but not in sidecar:
    //   - if explicitly dropped during the chain → propagate the drop
    //   - otherwise → leave alone (might be operator data we never tracked)
    const liveOnly = liveColls.filter((c) => !sidecarColls.includes(c));
    let propagatedDrops = 0;
    for (const collName of liveOnly) {
        if (!droppedCollections.includes(collName)) continue;
        try {
            await liveDb.collection(collName).drop();
            propagatedDrops++;
        } catch (err) {
            if (err?.codeName !== 'NamespaceNotFound') throw err;
        }
    }

    return { swapped, propagatedDrops };
}

/**
 * Sidecar variant of restoreFull. Replays the chain into a fresh
 * `<dbName>__crashsafe_restore_<id>` DB, runs the full pre-flight + replay
 * sequence there, and only swaps the live DB on full success. A failure
 * during replay leaves the live DB byte-for-byte unchanged.
 */
async function restoreFullSidecar(dbType, dbName, backupId, opts = {}) {
    const dir = dbBackupDir(dbType);
    const chain = getChainUpTo(dir, backupId ?? findEntry(dir, null)?.id);
    if (!chain.length) {
        throw new Error(`No backup chain found for ${dbType} up to ${backupId ?? '(latest)'}`);
    }

    logger.info({ dbType, dbName, steps: chain.length, mode: 'sidecar' }, 'Starting sidecar restore');

    // Pre-flight against the chain — exact same checks as direct mode.
    await preflightChain(dbType, dir, chain, opts);

    const runId = new Date().toISOString();
    const sidecarName = sidecarDbName(dbName, runId);

    const client = await getClient(config.restoreUri);

    // Clean up any orphan sidecars from previous failed runs so we start fresh.
    // This is auto-cleanup (not manual) because operators almost always want
    // the daemon to recover itself rather than be blocked on a stranded
    // sidecar from a failure they may not even remember.
    await cleanupOrphanSidecars(client, dbName);

    const sidecarDb = client.db(sidecarName);

    // Track which collections were marked as drops in this chain — needed
    // during the swap phase to propagate drops to the live DB.
    const droppedDuringChain = [];

    let step = 0;
    let totalUpserts = 0;
    let totalDeletes = 0;

    try {
        for (const entry of chain) {
            updateLockProgress({
                currentDb: dbType,
                phase: 'replaying-sidecar',
                currentEntry: entry.id,
                currentEntryType: entry.type,
                processedSteps: step,
                totalSteps: chain.length,
                sidecarName,
            });

            // Read the entry's tracking file ahead of time so we can record
            // any drop ops for the swap phase. The drops still execute via
            // replayEntry against the sidecar DB (where they're no-ops because
            // those collections never existed in the sidecar) — we just need
            // the metadata for the swap.
            if (entry.trackingFile) {
                const tp = safeJoin(dir, entry.trackingFile);
                if (fs.existsSync(tp)) {
                    const trackingData = EJSON.parse(fs.readFileSync(tp, 'utf8'));
                    for (const op of trackingData) {
                        if (op.op === 'drop') droppedDuringChain.push(op.collection);
                    }
                }
            }

            logger.info({ backupId: entry.id, type: entry.type, sidecarName }, 'Replaying entry into sidecar');
            const stats = await replayEntry(entry, dir, sidecarDb, {
                nsRename: { from: dbName, to: sidecarName },
            });
            totalUpserts += stats.upserts;
            totalDeletes += stats.deletes;
            step++;
        }

        // Swap phase
        updateLockProgress({ currentDb: dbType, phase: 'swapping', sidecarName, processedSteps: chain.length, totalSteps: chain.length });
        logger.info({ dbType, dbName, sidecarName }, 'Replay complete; swapping sidecar into live');
        const swapStats = await swapSidecarToLive(client, sidecarName, dbName, droppedDuringChain);

        // Drop the now-empty sidecar DB.
        try { await sidecarDb.dropDatabase(); }
        catch (err) {
            logger.warn({ err: redactErr(err), sidecarName }, 'Failed to drop empty sidecar after swap; safe to ignore but worth a manual check');
        }

        updateLockProgress({ currentDb: dbType, phase: 'done', processedSteps: chain.length, totalSteps: chain.length });
        logger.info({ upserts: totalUpserts, deletes: totalDeletes, ...swapStats }, 'Sidecar restore complete');
        return { upserts: totalUpserts, deletes: totalDeletes, mode: 'sidecar', sidecarName, ...swapStats };
    } catch (err) {
        // Replay failed before the swap could begin. Live DB is byte-for-byte
        // unchanged — this is the whole point of sidecar mode. Drop the
        // sidecar so disk doesn't accumulate orphans.
        logger.error({ err: redactErr(err), sidecarName }, 'Sidecar restore failed; live DB untouched, dropping sidecar');
        try { await sidecarDb.dropDatabase(); }
        catch (cleanupErr) {
            logger.error(
                { err: redactErr(cleanupErr), sidecarName },
                'Failed to drop sidecar after a failed restore. Manual cleanup required: drop database "' + sidecarName + '" or wait for the next sidecar restore which auto-cleans orphans.'
            );
        }
        throw err;
    }
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
async function restoreSince(dbType, dbName, sinceId, toId, opts = {}) {
    const dir = dbBackupDir(dbType);
    const chain = getChainFrom(dir, sinceId, toId ?? null);
    if (!chain.length) {
        throw new Error(`No backups found for ${dbType} from ${sinceId} to ${toId ?? '(latest)'}`);
    }

    logger.info({ dbType, dbName, sinceId, toId: toId ?? '(latest)', steps: chain.length }, 'Starting since-restore');

    // Pre-flight: even though restoreSince doesn't drop anything, a corrupt
    // tracking file mid-chain still produces a partial replay (some deletes
    // already applied, the rest never reached). Fail fast.
    await preflightChain(dbType, dir, chain, opts);

    // Restore destination respects RESTORE_URI when set; falls back to the
    // backup URI otherwise. All deleteMany / drop operations during the
    // restore go to the same destination as the mongorestore that follows.
    const db = await getDb(dbName, config.restoreUri);
    let totalUpserts = 0;
    let totalDeletes = 0;

    let step = 0;
    for (const entry of chain) {
        updateLockProgress({
            currentDb: dbType,
            phase: 'replaying',
            currentEntry: entry.id,
            currentEntryType: entry.type,
            processedSteps: step,
            totalSteps: chain.length,
        });
        logger.info({ backupId: entry.id, type: entry.type }, 'Replaying backup entry');
        const stats = await replayEntry(entry, dir, db);
        totalUpserts += stats.upserts;
        totalDeletes += stats.deletes;
        step++;
    }
    updateLockProgress({ currentDb: dbType, phase: 'done', processedSteps: chain.length, totalSteps: chain.length });

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
 * @param {string}               trigger   'cli' | 'api' | 'scheduled' | 'unknown'
 * @param {object}               [opts]    Extra options
 * @param {boolean}              [opts.verifyChecksums=false]  If true, the
 *   pre-flight check re-hashes every file with stored checksums against disk.
 *   Slow on large chains, but catches silent on-disk corruption before the
 *   live DB is wiped. Default off for fast recovery; on for paranoid restores.
 * @param {boolean}              [opts.dryRun=false]  If true, list the chain
 *   and operations that would run without touching the live DB or invoking
 *   mongorestore. The pre-flight chain check still runs (it's read-only).
 * @param {'direct'|'sidecar'}   [opts.mode='direct']  Restore strategy.
 *   `direct` (default) drops the live DB first and replays into it — fast,
 *   no extra disk, but a mid-replay failure leaves a half-restored DB.
 *   `sidecar` replays the chain into a shadow DB and only swaps on success —
 *   2× disk and 2× time, but the live DB is byte-for-byte unchanged on any
 *   replay failure. Sidecar implies dropExisting; the dropExisting flag is
 *   ignored when mode='sidecar'. Sidecar is also less destructive: live
 *   collections that were never backed up (e.g. operator data without the
 *   sensor prefix) are preserved, where direct mode wipes them.
 */
async function runRestore(target, backupId, full, sinceId, dropExisting, trigger = 'unknown', opts = {}) {
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

    const dryRun = !!opts.dryRun;
    const verifyChecksums = !!opts.verifyChecksums;
    const restoreMode = opts.mode === 'sidecar' ? 'sidecar' : 'direct';
    if (opts.mode && !['direct', 'sidecar'].includes(opts.mode)) {
        throw new Error(`Invalid restore mode "${opts.mode}" — expected 'direct' or 'sidecar'`);
    }

    // Dry-run path: planning only, no lock, no mutations. We still run the
    // pre-flight against each chain so the operator sees a realistic
    // "would this restore actually be allowed?" answer — including any
    // safeJoin or chain-broken refusals — without touching the live DB.
    if (dryRun) {
        const plan = {
            dryRun: true,
            target,
            backupId: backupId ?? null,
            sinceId: sinceId ?? null,
            full,
            dropExisting: !!dropExisting,
            mode: restoreMode,
            verifyChecksums,
            restoreUri: redactUri(config.restoreUri),
            dbs: [],
        };
        for (const { dbType, dbName } of targets) {
            const dir = path.resolve(config.backupDir, dbType);
            let chain;
            if (sinceId) {
                chain = getChainFrom(dir, sinceId, backupId ?? null);
            } else {
                chain = getChainUpTo(dir, backupId ?? findEntry(dir, null)?.id);
            }
            // Run the read-only pre-flight so chain corruption surfaces here too.
            try { await preflightChain(dbType, dir, chain, { verifyChecksums }); }
            catch (err) {
                plan.dbs.push({ dbType, dbName, error: err.message });
                continue;
            }
            plan.dbs.push({
                dbType,
                dbName,
                chainLength: chain.length,
                chain: chain.map((e) => ({
                    id: e.id,
                    type: e.type,
                    hasFile: !!e.file,
                    hasTracking: !!e.trackingFile,
                })),
            });
        }
        return plan;
    }

    if (inFlight) {
        logger.warn({ trigger }, 'Restore skipped: another restore is already running in this process');
        return { skipped: true, reason: 'in-process' };
    }

    const acquired = tryAcquireLock('restore', trigger);
    if (!acquired.ok) {
        logger.warn({ trigger, holder: acquired.holder }, 'Restore skipped: another operation holds the lock');
        return { skipped: true, reason: 'cross-process', holder: acquired.holder };
    }

    const startedAt = new Date().toISOString();
    const mode = sinceId ? 'since' : (full ? 'full' : 'incremental');
    updateLockProgress({ mode, target, backupId: backupId ?? null, sinceId: sinceId ?? null, dropExisting: !!dropExisting, verifyChecksums });

    inFlight = (async () => {
        try {
            for (const { dbType, dbName } of targets) {
                if (restoreMode === 'sidecar') {
                    // Sidecar mode is only meaningful for full restores (the
                    // whole point is "replace everything atomically"). Reject
                    // combinations that don't make sense.
                    if (sinceId) {
                        throw new Error('Sidecar mode is incompatible with --since (no atomic swap semantic for partial replay)');
                    }
                    if (!full) {
                        throw new Error('Sidecar mode requires --full (it always replays the whole chain)');
                    }
                    await restoreFullSidecar(dbType, dbName, backupId, { verifyChecksums });
                } else if (sinceId) {
                    await restoreSince(dbType, dbName, sinceId, backupId ?? null, { verifyChecksums });
                } else if (full) {
                    await restoreFull(dbType, dbName, backupId, dropExisting, { verifyChecksums });
                } else {
                    await restoreIncremental(dbType, dbName, backupId, { verifyChecksums });
                }
            }
            lastRestore = {
                trigger,
                mode,
                restoreMode,
                target,
                backupId: backupId ?? null,
                sinceId: sinceId ?? null,
                dropExisting: !!dropExisting,
                verifyChecksums,
                startedAt,
                finishedAt: new Date().toISOString(),
                status: 'success',
            };
        } catch (err) {
            lastRestore = {
                trigger,
                mode,
                restoreMode,
                target,
                backupId: backupId ?? null,
                sinceId: sinceId ?? null,
                dropExisting: !!dropExisting,
                verifyChecksums,
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

module.exports = { runRestore, getRestoreStats };
