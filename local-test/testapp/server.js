'use strict';
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { MongoClient, ObjectId, BSON: { EJSON } } = require('mongodb');

const URI                 = process.env.MONGO_URI || 'mongodb://admin:password@mongodb/?authSource=admin';
const DATA_DB             = process.env.DATA_DB || 'owdata';
const PARSE_DB            = process.env.PARSE_DB || 'parse';
const COLLECTION_PREFIX   = process.env.COLLECTION_PREFIX || 'sensors---';
const CONFIG_COLLECTION   = process.env.CONFIG_COLLECTION || 'config';
const PORT                = parseInt(process.env.PORT || '3001', 10);

// Crashsafe backend used by the auto-test runner.
const CRASHSAFE_URL       = (process.env.CRASHSAFE_URL || 'http://crashsafe:3000').replace(/\/$/, '');
const CRASHSAFE_AUTH_USER = process.env.CRASHSAFE_AUTH_USER || '';
const CRASHSAFE_AUTH_PASS = process.env.CRASHSAFE_AUTH_PASSWORD || '';

const TARGETED_DBS = [DATA_DB, PARSE_DB];

let client;

async function connect() {
    client = new MongoClient(URI, { serverSelectionTimeoutMS: 10_000 });
    await client.connect();
    console.log('Connected to MongoDB at', URI.replace(/\/\/[^@]*@/, '//***@'));
}

function readJsonBody(req) {
    return new Promise((resolve, reject) => {
        const chunks = [];
        req.on('data', c => chunks.push(c));
        req.on('end', () => {
            const raw = Buffer.concat(chunks).toString('utf-8');
            if (!raw) return resolve({});
            try { resolve(JSON.parse(raw)); }
            catch (e) { reject(new Error('Invalid JSON body')); }
        });
        req.on('error', reject);
    });
}

function jsonResponse(res, code, data) {
    res.writeHead(code, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(data));
}

// ---------------------------------------------------------------------------
// Data helpers
// ---------------------------------------------------------------------------

function randomSensorDoc(sensorId) {
    const now = new Date();
    return {
        sensorId: sensorId ?? ('sensor-' + Math.floor(Math.random() * 100)),
        value: Math.round(Math.random() * 1000) / 10,
        unit: ['°C', '%RH', 'hPa', 'lux'][Math.floor(Math.random() * 4)],
        timestamp: now,
        updatedAt: now,
    };
}

function randomParseDoc() {
    const now = new Date();
    return {
        objectId: 'obj-' + Math.random().toString(36).slice(2, 10),
        title: 'Item ' + Math.floor(Math.random() * 1000),
        active: Math.random() > 0.5,
        createdAt: now,
        updatedAt: now,
    };
}

function makeDoc(dbName, collName) {
    if (dbName === DATA_DB && collName === CONFIG_COLLECTION) {
        const now = new Date();
        return {
            key: 'cfg-' + Math.random().toString(36).slice(2, 6),
            value: Math.random().toString(36).slice(2),
            updatedAt: now,
        };
    }
    if (dbName === DATA_DB) return randomSensorDoc();
    return randomParseDoc();
}

// ---------------------------------------------------------------------------
// API
// ---------------------------------------------------------------------------

async function getStatus() {
    const out = { connected: true, dbs: [], totals: { collections: 0, documents: 0 } };
    for (const dbName of TARGETED_DBS) {
        const db = client.db(dbName);
        const colls = await db.listCollections().toArray();
        const collections = [];
        for (const c of colls) {
            let count = -1;
            try { count = await db.collection(c.name).estimatedDocumentCount(); } catch {}
            collections.push({
                name: c.name,
                count,
                isSensor: dbName === DATA_DB && c.name.startsWith(COLLECTION_PREFIX),
                isConfig: dbName === DATA_DB && c.name === CONFIG_COLLECTION,
                isBackedUp: dbName !== DATA_DB
                    || c.name === CONFIG_COLLECTION
                    || c.name.startsWith(COLLECTION_PREFIX),
            });
            if (count >= 0) out.totals.documents += count;
        }
        collections.sort((a, b) => a.name.localeCompare(b.name));
        out.totals.collections += collections.length;
        out.dbs.push({ name: dbName, collections });
    }
    return out;
}

/**
 * Ensure an `updatedAt` index exists on a collection. The production crashsafe
 * tool does NOT auto-index (intentionally — index builds on huge live collections
 * can be disruptive and the user controls index policy). But for the local test
 * harness it's a foot-gun to forget: append-only mode and incremental change
 * detection both depend on this index, and without it every collection scan is
 * a COLLSCAN.
 *
 * Call this from every place that creates or seeds a backed-up collection.
 * Idempotent — `createIndex` is a no-op when the index already exists.
 */
async function ensureUpdatedAtIndex(dbName, collName) {
    try {
        await client.db(dbName).collection(collName).createIndex({ updatedAt: 1 });
    } catch (err) {
        console.warn('Failed to ensure updatedAt index on', dbName + '/' + collName, '-', err?.message ?? err);
    }
}

async function insertDocs(dbName, collName, count) {
    if (!TARGETED_DBS.includes(dbName)) throw new Error('Unknown database: ' + dbName);
    if (!collName) throw new Error('collection required');
    const n = Math.max(1, Math.min(parseInt(count, 10) || 1, 100_000));
    const coll = client.db(dbName).collection(collName);
    const docs = [];
    for (let i = 0; i < n; i++) docs.push(makeDoc(dbName, collName));
    const result = await coll.insertMany(docs, { ordered: false });
    await ensureUpdatedAtIndex(dbName, collName);
    return { inserted: result.insertedCount };
}

async function modifyRandom(dbName, collName, count) {
    if (!collName) throw new Error('collection required');
    const n = Math.max(1, Math.min(parseInt(count, 10) || 1, 10_000));
    const coll = client.db(dbName).collection(collName);
    const sample = await coll.aggregate([{ $sample: { size: n } }]).toArray();
    if (!sample.length) return { modified: 0 };
    const ids = sample.map(d => d._id);
    const result = await coll.updateMany(
        { _id: { $in: ids } },
        { $set: { value: Math.round(Math.random() * 1000) / 10, updatedAt: new Date() } }
    );
    return { modified: result.modifiedCount };
}

async function deleteRandom(dbName, collName, count) {
    if (!collName) throw new Error('collection required');
    const n = Math.max(1, Math.min(parseInt(count, 10) || 1, 10_000));
    const coll = client.db(dbName).collection(collName);
    const sample = await coll.aggregate([{ $sample: { size: n } }]).toArray();
    if (!sample.length) return { deleted: 0 };
    const ids = sample.map(d => d._id);
    const result = await coll.deleteMany({ _id: { $in: ids } });
    return { deleted: result.deletedCount };
}

async function dropCollection(dbName, collName) {
    const db = client.db(dbName);
    try { await db.collection(collName).drop(); }
    catch (e) { if (e.codeName !== 'NamespaceNotFound') throw e; }
    return { dropped: collName };
}

async function wipeDb(dbName) {
    const db = client.db(dbName);
    const colls = await db.listCollections().toArray();
    let dropped = 0;
    for (const c of colls) {
        try { await db.collection(c.name).drop(); dropped++; } catch {}
    }
    return { dropped };
}

async function createCollection(dbName, name) {
    if (!name) throw new Error('name required');
    await client.db(dbName).createCollection(name).catch(() => {});
    return { created: name };
}

async function getSample(dbName, collName, limit) {
    const lim = Math.max(1, Math.min(parseInt(limit, 10) || 10, 200));
    const coll = client.db(dbName).collection(collName);
    const docs = await coll.find({}).sort({ _id: -1 }).limit(lim).toArray();
    return { docs };
}

/**
 * One-click setup: makes a config collection + several sensor collections in
 * the data DB, plus one parse collection, each pre-populated with 100 docs.
 */
async function setupDemo() {
    const sensors = ['temp-1', 'temp-2', 'humidity-1', 'pressure-1'];
    const log = [];

    // Config collection in data DB
    const cfg = client.db(DATA_DB).collection(CONFIG_COLLECTION);
    const cfgDocs = [];
    for (let i = 0; i < 5; i++) cfgDocs.push(makeDoc(DATA_DB, CONFIG_COLLECTION));
    await cfg.insertMany(cfgDocs);
    await ensureUpdatedAtIndex(DATA_DB, CONFIG_COLLECTION);
    log.push({ collection: `${DATA_DB}/${CONFIG_COLLECTION}`, inserted: cfgDocs.length });

    for (const name of sensors) {
        const fullName = COLLECTION_PREFIX + name;
        const coll = client.db(DATA_DB).collection(fullName);
        const docs = [];
        for (let i = 0; i < 100; i++) docs.push(randomSensorDoc('sensor-' + name));
        await coll.insertMany(docs);
        await ensureUpdatedAtIndex(DATA_DB, fullName);
        log.push({ collection: `${DATA_DB}/${fullName}`, inserted: 100 });
    }

    // One parse collection
    const parseColl = client.db(PARSE_DB).collection('items');
    const parseDocs = [];
    for (let i = 0; i < 100; i++) parseDocs.push(randomParseDoc());
    await parseColl.insertMany(parseDocs);
    await ensureUpdatedAtIndex(PARSE_DB, 'items');
    log.push({ collection: `${PARSE_DB}/items`, inserted: 100 });

    // A non-sensor collection in data DB to verify the prefix filter
    const stray = client.db(DATA_DB).collection('not_a_sensor');
    await stray.insertOne({ note: 'this should NOT appear in backups', updatedAt: new Date() });
    log.push({ collection: `${DATA_DB}/not_a_sensor`, inserted: 1, note: 'should be excluded by prefix filter' });

    return { results: log };
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Auto-test runner — drives crashsafe through a full backup/restore cycle and
// verifies data integrity by hashing collection contents before/after.
// ---------------------------------------------------------------------------

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function csFetch(pathPart, opts = {}) {
    const url = CRASHSAFE_URL + pathPart;
    const headers = { 'Content-Type': 'application/json', ...(opts.headers || {}) };
    if (CRASHSAFE_AUTH_USER && CRASHSAFE_AUTH_PASS) {
        headers['Authorization'] = 'Basic ' + Buffer.from(CRASHSAFE_AUTH_USER + ':' + CRASHSAFE_AUTH_PASS).toString('base64');
    }
    const res = await fetch(url, { ...opts, headers });
    if (!res.ok) {
        const body = await res.text().catch(() => '');
        throw new Error('crashsafe ' + pathPart + ' -> ' + res.status + ' ' + body.slice(0, 200));
    }
    const ct = res.headers.get('content-type') || '';
    return ct.includes('application/json') ? res.json() : res.text();
}

/**
 * Hash a collection's contents in a stable order. Returns { count, hash }.
 *
 * Uses **canonical EJSON** (mongo's `EJSON.stringify` with `relaxed: false`),
 * not plain JSON.stringify, because the latter is lossy on BSON sondertypes:
 * `Date` collapses to an ISO string, `ObjectId`/`Decimal128`/`Binary` to plain
 * strings. A backup→restore that silently changes a Date into a string would
 * NOT be caught by JSON.stringify-based hashing — both serialize identically.
 *
 * Keys are sorted recursively before stringifying so a roundtrip that reorders
 * fields (mongorestore is allowed to) doesn't produce a false mismatch.
 */
async function fingerprintCollection(dbName, collName) {
    const docs = await client.db(dbName).collection(collName).find({}).sort({ _id: 1 }).toArray();
    const hash = crypto.createHash('sha256');
    for (const doc of docs) {
        hash.update(EJSON.stringify(sortKeysDeep(doc), { relaxed: false }));
    }
    return { count: docs.length, hash: hash.digest('hex') };
}

/**
 * Recursively sort object keys. Arrays keep their order (order is semantic in
 * BSON arrays); plain objects get keys lexicographically sorted. BSON typed
 * values (ObjectId, Date, Decimal128, etc.) are not plain Objects — they're
 * passed through untouched so EJSON's serializer can identify them.
 */
function sortKeysDeep(value) {
    if (Array.isArray(value)) return value.map(sortKeysDeep);
    if (value && typeof value === 'object' && value.constructor === Object) {
        const out = {};
        for (const k of Object.keys(value).sort()) out[k] = sortKeysDeep(value[k]);
        return out;
    }
    return value;
}

async function fingerprintAll() {
    const out = {};
    for (const dbName of TARGETED_DBS) {
        const colls = await client.db(dbName).listCollections().toArray();
        out[dbName] = {};
        for (const c of colls) {
            out[dbName][c.name] = await fingerprintCollection(dbName, c.name);
        }
    }
    return out;
}

/**
 * Compare two fingerprints. Returns array of human-readable diff lines.
 * @param {Set<string>} ignoreCollections collection names that may differ (e.g. excluded-from-backup)
 */
function compareFingerprints(actual, expected, ignoreCollections = new Set()) {
    const issues = [];
    for (const dbName of Object.keys(expected)) {
        for (const coll of Object.keys(expected[dbName])) {
            if (ignoreCollections.has(coll)) continue;
            const exp = expected[dbName][coll];
            const act = actual[dbName]?.[coll];
            if (!act) {
                issues.push('Missing collection ' + dbName + '/' + coll + ' (expected ' + exp.count + ' docs)');
            } else if (act.count !== exp.count) {
                issues.push('Count mismatch ' + dbName + '/' + coll + ': expected ' + exp.count + ', got ' + act.count);
            } else if (act.hash !== exp.hash) {
                issues.push('Content mismatch ' + dbName + '/' + coll + ' (count matches but document data differs)');
            }
        }
    }
    for (const dbName of Object.keys(actual)) {
        for (const coll of Object.keys(actual[dbName])) {
            if (ignoreCollections.has(coll)) continue;
            if (!expected[dbName]?.[coll]) {
                issues.push('Unexpected collection present: ' + dbName + '/' + coll + ' (' + actual[dbName][coll].count + ' docs)');
            }
        }
    }
    return issues;
}

// ---- Test state ----------------------------------------------------------

let testState = null; // null = never run; populated when running or finished

function newStep(id, label) {
    return { id, label, status: 'pending', startedAt: null, finishedAt: null, error: null, details: null };
}

function logEvent(level, message) {
    if (!testState) return;
    testState.log.push({ ts: new Date().toISOString(), level, message });
    if (testState.log.length > 300) testState.log.shift();
}

async function runStep(step, fn) {
    step.status = 'running';
    step.startedAt = new Date().toISOString();
    logEvent('info', '▶ ' + step.label);
    try {
        const details = await fn();
        step.details = details ?? null;
        step.status = 'pass';
        step.finishedAt = new Date().toISOString();
        const detailHint = (details && typeof details === 'object')
            ? ' — ' + Object.entries(details).slice(0, 3).map(([k, v]) => k + '=' + JSON.stringify(v)).join(', ')
            : '';
        logEvent('success', '✓ ' + step.label + detailHint);
    } catch (err) {
        step.status = 'fail';
        step.error = err?.message ?? String(err);
        step.finishedAt = new Date().toISOString();
        logEvent('error', '✗ ' + step.label + ': ' + step.error);
        throw err;
    }
}

/**
 * Wait for a backup or restore operation to complete (lockfile released and
 * the relevant `lastRuns`/`lastRestore` finishedAt is newer than baseline).
 */
async function waitForCompletion(kind, baseline, timeoutMs = 10 * 60 * 1000) {
    const start = Date.now();
    let lastSeenInFlight = false;
    while (Date.now() - start < timeoutMs) {
        const status = await csFetch('/api/status');
        const inFlight = status.inFlight;
        if (inFlight) lastSeenInFlight = true;

        if (kind === 'backup') {
            const r = status.runs?.manual ?? null;
            if (!inFlight && r && r.finishedAt !== baseline?.finishedAt) {
                if (r.status !== 'success') throw new Error('Backup ended with status: ' + r.status + ' — ' + (r.error || ''));
                return r;
            }
        } else if (kind === 'restore') {
            const r = status.lastRestore ?? null;
            if (!inFlight && r && r.finishedAt !== baseline?.finishedAt) {
                if (r.status !== 'success') throw new Error('Restore ended with status: ' + r.status + ' — ' + (r.error || ''));
                return r;
            }
        } else if (kind === 'verify') {
            const r = status.lastVerify ?? null;
            // Verify uses three statuses: success, warnings, failure. Caller
            // decides which it expects — return the record either way.
            if (!inFlight && r && r.finishedAt !== baseline?.finishedAt) {
                return r;
            }
        }
        await sleep(1500);
    }
    throw new Error('Timed out after ' + (timeoutMs / 1000) + 's waiting for ' + kind);
}

async function runAutoTest() {
    if (testState && testState.running) {
        throw new Error('A test run is already in progress');
    }
    const steps = {
        wipe1:        newStep('wipe-initial',     'Wipe both databases (clean slate)'),
        seed:         newStep('seed-initial',     'Seed initial dataset (state A)'),
        fpA:          newStep('fp-a',             'Fingerprint state A'),
        full:         newStep('full-backup',      'Trigger full backup'),
        modify:       newStep('modify',           'Modify data (insert + update + delete + new collection)'),
        fpB:          newStep('fp-b',             'Fingerprint state B'),
        inc:          newStep('inc-backup',       'Trigger incremental backup'),
        wipe2:        newStep('wipe-rl',          'Wipe before Restore to Latest'),
        rl:           newStep('restore-latest',   'Trigger Restore to Latest'),
        verifyRL:     newStep('verify-rl',        'Verify restored DB matches state B (excluding non-backed-up data)'),
        wipe3:        newStep('wipe-rp',          'Wipe before Restore to Point (full)'),
        rp:           newStep('restore-point',    'Trigger Restore to Point (the full backup)'),
        verifyRP:     newStep('verify-rp',        'Verify restored DB matches state A (excluding non-backed-up data)'),
        verifyExcl:   newStep('verify-excl',      'Verify excluded collection (not_a_sensor) absent in both restores'),

        // Phase H — Multi-Inc PITR (the original user bug: PITR to a specific Inc must yield that exact state)
        rl2:          newStep('rl-state-b',       'Restore to Latest (set up state B before next mutation)'),
        modify2:      newStep('modify-2',         'Mutate again to state C (different from B)'),
        fpC:          newStep('fp-c',             'Fingerprint state C'),
        inc2:         newStep('inc-2',            'Trigger second incremental backup (chain: Full, Inc1, Inc2)'),
        wipe4:        newStep('wipe-pitr-inc1',   'Wipe before PITR to Inc1'),
        pitrInc1:     newStep('pitr-inc1',        'Restore to Point — Inc1 (must yield state B, NOT state C)'),
        verifyB2:     newStep('verify-b2',        'Verify restored DB == state B'),
        wipe5:        newStep('wipe-pitr-inc2',   'Wipe before PITR to Inc2'),
        pitrInc2:     newStep('pitr-inc2',        'Restore to Point — Inc2 (must yield state C)'),
        verifyC:      newStep('verify-c',         'Verify restored DB == state C'),

        // Phase I — New Full creates a checkpoint that older entries don't bleed into
        modify3:      newStep('modify-3',         'Mutate further to state D (extra collection added)'),
        fpD:          newStep('fp-d',             'Fingerprint state D'),
        full2:        newStep('full-2',           'Trigger second full backup (new checkpoint)'),
        wipe6:        newStep('wipe-pitr-full2',  'Wipe before PITR to Full2'),
        pitrFull2:    newStep('pitr-full2',       'Restore to Point — Full2 (must yield state D, no leakage from older Full/Inc)'),
        verifyD:      newStep('verify-d',         'Verify restored DB == state D (chain checkpoint semantics)'),

        // Phase K — Config collection delete tracking (regression test for the
        // "config delete is silently skipped on restore" bug)
        modifyCfg:    newStep('modify-config',    'Delete + modify config docs (state E)'),
        fpE:          newStep('fp-e',             'Fingerprint state E'),
        inc3:         newStep('inc-3',            'Trigger third incremental (after config mutation)'),
        wipe7:        newStep('wipe-pitr-inc3',   'Wipe before PITR to Inc3'),
        pitrInc3:     newStep('pitr-inc3',        'Restore to Point — Inc3 (config delete must be honoured)'),
        verifyE:      newStep('verify-e',         'Verify restored DB == state E (config has 4 docs, not 5)'),

        // Phase L — Collection-drop tracking (regression: a dropped collection
        // must not silently re-appear on restore via the prior chain entry's dump)
        prepDrop:     newStep('prep-drop',        'Create throwaway sensor collection (sensors---to-drop)'),
        incTrack:     newStep('inc-4-track',      'Inc backup — captures the new collection'),
        dropColl:     newStep('drop-coll',        'Drop the throwaway collection'),
        incDrop:      newStep('inc-5-drop',       'Inc backup — must record collection-drop tracking op'),
        wipe8:        newStep('wipe-drop',        'Wipe before Restore to Latest (drop test)'),
        restoreDrop:  newStep('restore-drop',     'Restore to Latest after drop'),
        verifyDrop:   newStep('verify-drop',      'Verify dropped collection is absent (not silently restored)'),

        // Phase M — Parse-DB delete tracking. Parse never has APPEND_ONLY enabled,
        // so this is the canonical positive test that per-document delete tracking
        // works on a non-config, non-append-only collection.
        modifyParse:  newStep('modify-parse',     'Delete + modify + insert on parse/items (state F)'),
        fpF:          newStep('fp-f',             'Fingerprint state F'),
        incParse:     newStep('inc-6-parse',      'Inc backup — must capture parse deletes'),
        wipe9:        newStep('wipe-parse',       'Wipe before Restore to Latest (parse-delete test)'),
        restoreParse: newStep('restore-parse',    'Restore to Latest after parse mutations'),
        verifyParse:  newStep('verify-parse',     'Verify parse/items deletes honoured by restore'),

        // Phase N — Index preservation across the dump/restore round-trip.
        // mongodump writes per-collection metadata.json with index defs; if a
        // future change ever drops that, we want the auto-test to scream.
        verifyIndexes: newStep('verify-indexes',  'Verify updatedAt indexes exist on backed-up collections after restore'),

        // Phase O — Integrity verification (SHA-256 round-trip + corruption detection).
        // First trigger a clean verify to confirm every backup written this run
        // has matching checksums. Then corrupt a single byte in a real dump
        // file and re-verify — the second pass MUST report it as corrupt.
        // Finally restore the byte and re-verify, so the test leaves the
        // backup tree consistent for any follow-up run.
        verifyCleanRun:  newStep('verify-clean',    'Trigger /api/verify and confirm all written backups pass'),
        corruptBackup:   newStep('corrupt-backup',  'Corrupt one byte in a real .bson.gz dump file'),
        verifyDetectsCorruption: newStep('verify-detects', 'Trigger /api/verify and confirm the corruption is detected'),
        repairBackup:    newStep('repair-backup',   'Restore the byte to leave the backup tree consistent'),
        verifyRepaired:  newStep('verify-repaired', 'Trigger /api/verify and confirm everything is clean again'),

        // Phase J — Manifest audit
        audit:        newStep('audit',            'Audit: verify backup count and size tracking'),
    };
    testState = {
        running: true,
        startedAt: new Date().toISOString(),
        finishedAt: null,
        steps: Object.values(steps),
        log: [],
        summary: null,
    };
    logEvent('info', 'Auto-test started');
    logEvent('info', 'Crashsafe URL: ' + CRASHSAFE_URL);

    const ignoreColls = new Set(['not_a_sensor']);
    let fpA, fpB, fpC, fpD, fpE;
    let fullBackupId, inc1Id, inc2Id, full2Id, inc3Id;
    let fullBaseline, incBaseline, restoreBaseline;

    // Read the daemon's effective append-only config so the modify phase doesn't
    // create deletions the inc backup is configured to ignore (would break verifyRL).
    // Config-collection deletes are still tracked even with append-only on, so the
    // Phase K config-delete test works regardless.
    const initStatus = await csFetch('/api/status');
    const appendOnlyData = !!initStatus.config?.appendOnlyData;
    const appendOnlyParse = !!initStatus.config?.appendOnlyParse;
    logEvent('info', 'Daemon append-only flags: data=' + appendOnlyData + ', parse=' + appendOnlyParse);

    try {
        // ---- Phase A: clean slate + seed --------------------------------
        await runStep(steps.wipe1, async () => {
            const a = await wipeDb(DATA_DB);
            const b = await wipeDb(PARSE_DB);
            return { dataDropped: a.dropped, parseDropped: b.dropped };
        });

        await runStep(steps.seed, async () => {
            const inserts = {};
            // config (always backed up)
            const cfg = []; for (let i = 0; i < 5; i++) cfg.push(makeDoc(DATA_DB, CONFIG_COLLECTION));
            await client.db(DATA_DB).collection(CONFIG_COLLECTION).insertMany(cfg);
            await ensureUpdatedAtIndex(DATA_DB, CONFIG_COLLECTION);
            inserts[CONFIG_COLLECTION] = 5;
            // sensors
            const sensorSpec = { 'temp-1': 50, 'temp-2': 50, 'humidity-1': 30 };
            for (const [name, count] of Object.entries(sensorSpec)) {
                const docs = []; for (let i = 0; i < count; i++) docs.push(randomSensorDoc('sensor-' + name));
                const fullName = COLLECTION_PREFIX + name;
                await client.db(DATA_DB).collection(fullName).insertMany(docs);
                await ensureUpdatedAtIndex(DATA_DB, fullName);
                inserts[fullName] = count;
            }
            // excluded (NOT backed up — verifies prefix filter)
            const stray = []; for (let i = 0; i < 10; i++) stray.push({ payload: 'excluded-' + i, updatedAt: new Date() });
            await client.db(DATA_DB).collection('not_a_sensor').insertMany(stray);
            inserts['not_a_sensor'] = 10;
            // parse
            const items = []; for (let i = 0; i < 40; i++) items.push(randomParseDoc());
            await client.db(PARSE_DB).collection('items').insertMany(items);
            await ensureUpdatedAtIndex(PARSE_DB, 'items');
            inserts['parse/items'] = 40;
            return inserts;
        });

        await runStep(steps.fpA, async () => {
            fpA = await fingerprintAll();
            const collCount = Object.keys(fpA[DATA_DB] || {}).length + Object.keys(fpA[PARSE_DB] || {}).length;
            return { collections: collCount };
        });

        // ---- Phase B: full backup ---------------------------------------
        await runStep(steps.full, async () => {
            const before = await csFetch('/api/status');
            fullBaseline = before.runs?.manual ?? null;
            await csFetch('/api/trigger/backup', {
                method: 'POST',
                body: JSON.stringify({ type: 'full', target: 'all' }),
            });
            const r = await waitForCompletion('backup', fullBaseline);
            // After completion, find the new full backup id from manifest
            const after = await csFetch('/api/status');
            const dataBackups = after.backups?.find(b => b.type === 'data');
            fullBackupId = dataBackups?.lastBackup ?? null;
            if (!fullBackupId) throw new Error('Full backup completed but no id found in manifest');
            return { id: fullBackupId, finishedAt: r.finishedAt };
        });

        // ---- Phase C: modify --------------------------------------------
        await runStep(steps.modify, async () => {
            const t1 = client.db(DATA_DB).collection(COLLECTION_PREFIX + 'temp-1');
            const t2 = client.db(DATA_DB).collection(COLLECTION_PREFIX + 'temp-2');
            const hum = client.db(DATA_DB).collection(COLLECTION_PREFIX + 'humidity-1');

            // wait a moment so modified docs have updatedAt > full backup id
            await sleep(1100);

            const newDocs = []; for (let i = 0; i < 20; i++) newDocs.push(randomSensorDoc('sensor-temp-1'));
            await t1.insertMany(newDocs);

            const t2Sample = await t2.aggregate([{ $sample: { size: 10 } }]).toArray();
            await t2.updateMany(
                { _id: { $in: t2Sample.map(d => d._id) } },
                { $set: { value: 999, updatedAt: new Date() } }
            );

            // Deletes on a sensor collection only get captured by inc backups when
            // append-only mode is OFF. With append-only on, the inc skips delete
            // detection by design, so creating deletes here would make verifyRL fail
            // for a non-bug reason. Skip and report it.
            let deletedCount = 0;
            if (!appendOnlyData) {
                const humSample = await hum.aggregate([{ $sample: { size: 5 } }]).toArray();
                const res = await hum.deleteMany({ _id: { $in: humSample.map(d => d._id) } });
                deletedCount = res.deletedCount;
            }

            // brand-new collection (created after the full)
            const pres = []; for (let i = 0; i < 15; i++) pres.push(randomSensorDoc('sensor-pressure'));
            await client.db(DATA_DB).collection(COLLECTION_PREFIX + 'pressure-new').insertMany(pres);
            await ensureUpdatedAtIndex(DATA_DB, COLLECTION_PREFIX + 'pressure-new');

            return {
                added: 35,
                modified: 10,
                deleted: deletedCount,
                deleteSkippedReason: appendOnlyData ? 'data DB in append-only mode' : null,
                newCollections: 1,
            };
        });

        await runStep(steps.fpB, async () => {
            fpB = await fingerprintAll();
            return { collections: Object.keys(fpB[DATA_DB] || {}).length + Object.keys(fpB[PARSE_DB] || {}).length };
        });

        // ---- Phase D: inc backup ----------------------------------------
        await runStep(steps.inc, async () => {
            const before = await csFetch('/api/status');
            incBaseline = before.runs?.manual ?? null;
            await csFetch('/api/trigger/backup', {
                method: 'POST',
                body: JSON.stringify({ type: 'incremental', target: 'all' }),
            });
            const r = await waitForCompletion('backup', incBaseline);
            const after = await csFetch('/api/status');
            inc1Id = after.backups?.find(b => b.type === 'data')?.history?.[0]?.id ?? null;
            if (!inc1Id) throw new Error('Inc backup completed but no id found');
            return { id: inc1Id };
        });

        // ---- Phase E: Restore to Latest ---------------------------------
        await runStep(steps.wipe2, async () => {
            await wipeDb(DATA_DB);
            await wipeDb(PARSE_DB);
            return { ok: true };
        });

        await runStep(steps.rl, async () => {
            const before = await csFetch('/api/status');
            restoreBaseline = before.lastRestore ?? null;
            await csFetch('/api/trigger/restore', {
                method: 'POST',
                body: JSON.stringify({ type: 'full', target: 'all' }),
            });
            const r = await waitForCompletion('restore', restoreBaseline);
            return { finishedAt: r.finishedAt, mode: r.mode };
        });

        await runStep(steps.verifyRL, async () => {
            const after = await fingerprintAll();
            const issues = compareFingerprints(after, fpB, ignoreColls);
            if (issues.length) throw new Error('State B mismatch: ' + issues.join(' | '));
            return { match: true, comparedCollections: Object.keys(fpB[DATA_DB]).length + Object.keys(fpB[PARSE_DB]).length };
        });

        // ---- Phase F: Restore to Point (the full) -----------------------
        await runStep(steps.wipe3, async () => {
            await wipeDb(DATA_DB);
            await wipeDb(PARSE_DB);
            return { ok: true };
        });

        await runStep(steps.rp, async () => {
            const before = await csFetch('/api/status');
            restoreBaseline = before.lastRestore ?? null;
            await csFetch('/api/trigger/restore', {
                method: 'POST',
                body: JSON.stringify({ type: 'full', target: 'all', backupId: fullBackupId }),
            });
            const r = await waitForCompletion('restore', restoreBaseline);
            return { finishedAt: r.finishedAt, target: fullBackupId };
        });

        await runStep(steps.verifyRP, async () => {
            const after = await fingerprintAll();
            const issues = compareFingerprints(after, fpA, ignoreColls);
            if (issues.length) throw new Error('State A mismatch: ' + issues.join(' | '));
            return { match: true };
        });

        // ---- Phase G: exclusion audit ------------------------------------
        await runStep(steps.verifyExcl, async () => {
            const colls = await client.db(DATA_DB).listCollections().toArray();
            const found = colls.find(c => c.name === 'not_a_sensor');
            if (found) throw new Error('not_a_sensor was restored — prefix filter broken');
            return { ok: true, message: 'not_a_sensor correctly absent from restored data DB' };
        });

        // ---- Phase H: Multi-Inc PITR (covers the original user-reported bug) -----------
        // After Phase F we're at state A. Need to roll forward to state B first so we
        // can mutate to state C and create Inc2.

        await runStep(steps.rl2, async () => {
            const before = await csFetch('/api/status');
            restoreBaseline = before.lastRestore ?? null;
            await csFetch('/api/trigger/restore', {
                method: 'POST',
                body: JSON.stringify({ type: 'full', target: 'all' }),
            });
            const r = await waitForCompletion('restore', restoreBaseline);
            return { mode: r.mode };
        });

        await runStep(steps.modify2, async () => {
            await sleep(1100); // ensure updatedAt > Inc1.id
            const t1 = client.db(DATA_DB).collection(COLLECTION_PREFIX + 'temp-1');
            const more = []; for (let i = 0; i < 30; i++) more.push(randomSensorDoc('sensor-temp-1'));
            await t1.insertMany(more);

            const press = client.db(DATA_DB).collection(COLLECTION_PREFIX + 'pressure-new');
            const sample = await press.aggregate([{ $sample: { size: 5 } }]).toArray();
            if (sample.length) {
                await press.updateMany(
                    { _id: { $in: sample.map(d => d._id) } },
                    { $set: { value: 555, updatedAt: new Date() } }
                );
            }
            return { added: 30, modified: sample.length };
        });

        await runStep(steps.fpC, async () => {
            fpC = await fingerprintAll();
            return { collections: Object.keys(fpC[DATA_DB] || {}).length + Object.keys(fpC[PARSE_DB] || {}).length };
        });

        await runStep(steps.inc2, async () => {
            const before = await csFetch('/api/status');
            const baseline = before.runs?.manual ?? null;
            await csFetch('/api/trigger/backup', {
                method: 'POST',
                body: JSON.stringify({ type: 'incremental', target: 'all' }),
            });
            const r = await waitForCompletion('backup', baseline);
            const after = await csFetch('/api/status');
            inc2Id = after.backups?.find(b => b.type === 'data')?.history?.[0]?.id ?? null;
            if (!inc2Id) throw new Error('Inc2 backup completed but no id found');
            if (inc2Id === inc1Id) throw new Error('Inc2 has same id as Inc1 — manifest corruption');
            return { id: inc2Id };
        });

        await runStep(steps.wipe4, async () => {
            await wipeDb(DATA_DB);
            await wipeDb(PARSE_DB);
            return { ok: true };
        });

        await runStep(steps.pitrInc1, async () => {
            const before = await csFetch('/api/status');
            restoreBaseline = before.lastRestore ?? null;
            await csFetch('/api/trigger/restore', {
                method: 'POST',
                body: JSON.stringify({ type: 'full', target: 'all', backupId: inc1Id }),
            });
            const r = await waitForCompletion('restore', restoreBaseline);
            return { target: inc1Id };
        });

        await runStep(steps.verifyB2, async () => {
            const after = await fingerprintAll();
            const issues = compareFingerprints(after, fpB, ignoreColls);
            if (issues.length) {
                // If the result matched state C instead, that's the original user bug.
                const altIssues = compareFingerprints(after, fpC, ignoreColls);
                const tag = altIssues.length === 0 ? ' (BUG: PITR walked forward to state C instead of stopping at Inc1)' : '';
                throw new Error('Expected state B, got differences' + tag + ': ' + issues.slice(0, 3).join(' | '));
            }
            return { match: true };
        });

        await runStep(steps.wipe5, async () => {
            await wipeDb(DATA_DB);
            await wipeDb(PARSE_DB);
            return { ok: true };
        });

        await runStep(steps.pitrInc2, async () => {
            const before = await csFetch('/api/status');
            restoreBaseline = before.lastRestore ?? null;
            await csFetch('/api/trigger/restore', {
                method: 'POST',
                body: JSON.stringify({ type: 'full', target: 'all', backupId: inc2Id }),
            });
            const r = await waitForCompletion('restore', restoreBaseline);
            return { target: inc2Id };
        });

        await runStep(steps.verifyC, async () => {
            const after = await fingerprintAll();
            const issues = compareFingerprints(after, fpC, ignoreColls);
            if (issues.length) throw new Error('Expected state C, got differences: ' + issues.slice(0, 3).join(' | '));
            return { match: true };
        });

        // ---- Phase I: New Full creates a fresh checkpoint --------------------------------
        // We're currently at state C. Mutate further to state D, take Full2, restore — verify
        // that PITR to Full2 only includes state D and is NOT polluted by older Full/Inc data.

        await runStep(steps.modify3, async () => {
            await sleep(1100);
            const checkpoint = client.db(DATA_DB).collection(COLLECTION_PREFIX + 'checkpoint-marker');
            const docs = []; for (let i = 0; i < 25; i++) docs.push(randomSensorDoc('checkpoint-test'));
            await checkpoint.insertMany(docs);
            await ensureUpdatedAtIndex(DATA_DB, COLLECTION_PREFIX + 'checkpoint-marker');
            return { added: 25, newCollection: COLLECTION_PREFIX + 'checkpoint-marker' };
        });

        await runStep(steps.fpD, async () => {
            fpD = await fingerprintAll();
            return { collections: Object.keys(fpD[DATA_DB] || {}).length + Object.keys(fpD[PARSE_DB] || {}).length };
        });

        await runStep(steps.full2, async () => {
            const before = await csFetch('/api/status');
            const baseline = before.runs?.manual ?? null;
            await csFetch('/api/trigger/backup', {
                method: 'POST',
                body: JSON.stringify({ type: 'full', target: 'all' }),
            });
            const r = await waitForCompletion('backup', baseline);
            const after = await csFetch('/api/status');
            full2Id = after.backups?.find(b => b.type === 'data')?.history?.[0]?.id ?? null;
            if (!full2Id) throw new Error('Full2 backup completed but no id found');
            return { id: full2Id };
        });

        await runStep(steps.wipe6, async () => {
            await wipeDb(DATA_DB);
            await wipeDb(PARSE_DB);
            return { ok: true };
        });

        await runStep(steps.pitrFull2, async () => {
            const before = await csFetch('/api/status');
            restoreBaseline = before.lastRestore ?? null;
            await csFetch('/api/trigger/restore', {
                method: 'POST',
                body: JSON.stringify({ type: 'full', target: 'all', backupId: full2Id }),
            });
            const r = await waitForCompletion('restore', restoreBaseline);
            return { target: full2Id };
        });

        await runStep(steps.verifyD, async () => {
            const after = await fingerprintAll();
            const issues = compareFingerprints(after, fpD, ignoreColls);
            if (issues.length) {
                throw new Error('Expected state D after Full2 PITR (no older-chain leakage), got: ' + issues.slice(0, 3).join(' | '));
            }
            return { match: true };
        });

        // ---- Phase K: Config delete tracking ---------------------------------------------
        // Regression test for the original bug where deleting a config doc between two
        // backups was not reflected on restore (mongorestore silently skipped the
        // duplicate _ids from the prior chain replay). Now config also goes through
        // delete detection + full-set upsert tracking.
        //
        // Currently the DB is at state D (post-PITR-to-Full2). Mutate config: delete one
        // doc and modify another. Then take an inc, wipe, PITR-to-inc, and verify that:
        //   - the deleted config doc does NOT come back
        //   - the modified config doc has the new value (not the pre-mutation one)

        await runStep(steps.modifyCfg, async () => {
            await sleep(1100); // updatedAt > Full2.id
            const cfgColl = client.db(DATA_DB).collection(CONFIG_COLLECTION);

            const total = await cfgColl.estimatedDocumentCount();
            if (total < 2) throw new Error('Expected >= 2 config docs to exist for this test; got ' + total);

            // Pick a doc to delete and a different one to modify
            const sample = await cfgColl.aggregate([{ $sample: { size: 2 } }]).toArray();
            const deleted = await cfgColl.deleteOne({ _id: sample[0]._id });
            if (deleted.deletedCount !== 1) throw new Error('Failed to delete config doc');

            await cfgColl.updateOne(
                { _id: sample[1]._id },
                { $set: { value: 'CONFIG-MUTATED-MARKER', updatedAt: new Date() } }
            );

            const remaining = await cfgColl.estimatedDocumentCount();
            return {
                deletedConfigId: String(sample[0]._id),
                modifiedConfigId: String(sample[1]._id),
                configDocsRemaining: remaining,
            };
        });

        await runStep(steps.fpE, async () => {
            fpE = await fingerprintAll();
            return { configDocs: fpE[DATA_DB]?.[CONFIG_COLLECTION]?.count ?? null };
        });

        await runStep(steps.inc3, async () => {
            const before = await csFetch('/api/status');
            const baseline = before.runs?.manual ?? null;
            await csFetch('/api/trigger/backup', {
                method: 'POST',
                body: JSON.stringify({ type: 'incremental', target: 'all' }),
            });
            const r = await waitForCompletion('backup', baseline);
            const after = await csFetch('/api/status');
            inc3Id = after.backups?.find(b => b.type === 'data')?.history?.[0]?.id ?? null;
            if (!inc3Id) throw new Error('Inc3 backup completed but no id found');
            return { id: inc3Id };
        });

        await runStep(steps.wipe7, async () => {
            await wipeDb(DATA_DB);
            await wipeDb(PARSE_DB);
            return { ok: true };
        });

        await runStep(steps.pitrInc3, async () => {
            const before = await csFetch('/api/status');
            restoreBaseline = before.lastRestore ?? null;
            await csFetch('/api/trigger/restore', {
                method: 'POST',
                body: JSON.stringify({ type: 'full', target: 'all', backupId: inc3Id }),
            });
            const r = await waitForCompletion('restore', restoreBaseline);
            return { target: inc3Id };
        });

        await runStep(steps.verifyE, async () => {
            const after = await fingerprintAll();
            const expectedConfig = fpE[DATA_DB]?.[CONFIG_COLLECTION];
            const actualConfig = after[DATA_DB]?.[CONFIG_COLLECTION];

            if (!actualConfig) {
                throw new Error('Config collection missing after PITR — restore failed');
            }

            // Targeted regression check first, with an explicit message so the user
            // immediately knows it's the config-delete-tracking bug.
            if (actualConfig.count > expectedConfig.count) {
                throw new Error(
                    'Config delete not honoured by restore (got ' + actualConfig.count +
                    ' docs, expected ' + expectedConfig.count + '). The "deleted config doc resurrects on restore" bug.'
                );
            }
            if (actualConfig.count < expectedConfig.count) {
                throw new Error('Config has fewer docs than expected: ' + actualConfig.count + ' vs ' + expectedConfig.count);
            }

            // Full fingerprint match (catches the "modify wasn't tracked" case via hash mismatch).
            const issues = compareFingerprints(after, fpE, ignoreColls);
            if (issues.length) {
                const configIssue = issues.find(i => i.includes(CONFIG_COLLECTION));
                if (configIssue) {
                    throw new Error('Config modification not honoured by restore: ' + configIssue);
                }
                throw new Error('Expected state E, got: ' + issues.slice(0, 3).join(' | '));
            }
            return { configDocsAfter: actualConfig.count, configHashMatch: true };
        });

        // ---- Phase L: Collection-drop tracking ------------------------------------------
        // Create a sensor collection, back it up, drop the whole collection, back up again,
        // wipe + restore-latest, then verify the dropped collection does NOT come back.
        // Without drop tracking, the prior chain entry's dump silently re-creates it.
        const DOOMED_COLL = COLLECTION_PREFIX + 'to-drop';

        await runStep(steps.prepDrop, async () => {
            const docs = []; for (let i = 0; i < 10; i++) docs.push(randomSensorDoc('sensor-to-drop'));
            await client.db(DATA_DB).collection(DOOMED_COLL).insertMany(docs);
            await ensureUpdatedAtIndex(DATA_DB, DOOMED_COLL);
            return { collection: DOOMED_COLL, docs: 10 };
        });

        await runStep(steps.incTrack, async () => {
            const before = await csFetch('/api/status');
            const baseline = before.runs?.manual ?? null;
            await csFetch('/api/trigger/backup', {
                method: 'POST',
                body: JSON.stringify({ type: 'incremental', target: 'all' }),
            });
            await waitForCompletion('backup', baseline);
            return { ok: true };
        });

        await runStep(steps.dropColl, async () => {
            await sleep(1100); // ensure the next backup's id is strictly newer
            await client.db(DATA_DB).collection(DOOMED_COLL).drop();
            const colls = await client.db(DATA_DB).listCollections({ name: DOOMED_COLL }).toArray();
            if (colls.length) throw new Error('drop did not actually drop ' + DOOMED_COLL);
            return { dropped: DOOMED_COLL };
        });

        await runStep(steps.incDrop, async () => {
            const before = await csFetch('/api/status');
            const baseline = before.runs?.manual ?? null;
            await csFetch('/api/trigger/backup', {
                method: 'POST',
                body: JSON.stringify({ type: 'incremental', target: 'all' }),
            });
            await waitForCompletion('backup', baseline);
            return { ok: true };
        });

        await runStep(steps.wipe8, async () => {
            await wipeDb(DATA_DB);
            await wipeDb(PARSE_DB);
            return { ok: true };
        });

        await runStep(steps.restoreDrop, async () => {
            const before = await csFetch('/api/status');
            restoreBaseline = before.lastRestore ?? null;
            await csFetch('/api/trigger/restore', {
                method: 'POST',
                body: JSON.stringify({ type: 'full', target: 'all' }),
            });
            const r = await waitForCompletion('restore', restoreBaseline);
            return { mode: r.mode };
        });

        await runStep(steps.verifyDrop, async () => {
            const colls = await client.db(DATA_DB).listCollections({ name: DOOMED_COLL }).toArray();
            if (colls.length) {
                const count = await client.db(DATA_DB).collection(DOOMED_COLL).estimatedDocumentCount();
                throw new Error(
                    'Dropped collection ' + DOOMED_COLL + ' was silently restored (' + count + ' docs). ' +
                    'Drop-tracking is missing or broken.'
                );
            }
            return { ok: true, droppedCollectionAbsent: DOOMED_COLL };
        });

        // ---- Phase M: Parse-DB delete tracking -----------------------------------------
        // The data DB has APPEND_ONLY enabled in this compose, so its sensor-collection
        // deletes are intentionally untested. Parse has full delete tracking and is the
        // canonical positive case: a deleted parse/items doc must NOT come back on restore.
        let fpF;
        let parseDeletedIds;
        await runStep(steps.modifyParse, async () => {
            await sleep(1100); // updatedAt > prev backup id
            const items = client.db(PARSE_DB).collection('items');

            // Sample 5 to delete and 1 different one to modify
            const total = await items.estimatedDocumentCount();
            if (total < 8) throw new Error('Expected >= 8 parse/items docs for the test, got ' + total);
            const sample = await items.aggregate([{ $sample: { size: 6 } }]).toArray();
            const toDelete = sample.slice(0, 5).map(d => d._id);
            const toModifyId = sample[5]._id;

            const delRes = await items.deleteMany({ _id: { $in: toDelete } });
            if (delRes.deletedCount !== 5) throw new Error('Expected to delete 5 parse docs, deleted ' + delRes.deletedCount);
            await items.updateOne(
                { _id: toModifyId },
                { $set: { value: 'PARSE-MUTATED-MARKER', updatedAt: new Date() } }
            );

            // Insert a couple of fresh ones so the inc isn't only deletes
            const fresh = []; for (let i = 0; i < 3; i++) fresh.push(randomParseDoc());
            await items.insertMany(fresh);

            parseDeletedIds = toDelete;
            return { deleted: 5, modified: 1, inserted: 3 };
        });

        await runStep(steps.fpF, async () => {
            fpF = await fingerprintAll();
            return { parseItemsCount: fpF[PARSE_DB]?.items?.count ?? null };
        });

        await runStep(steps.incParse, async () => {
            const before = await csFetch('/api/status');
            const baseline = before.runs?.manual ?? null;
            await csFetch('/api/trigger/backup', {
                method: 'POST',
                body: JSON.stringify({ type: 'incremental', target: 'all' }),
            });
            await waitForCompletion('backup', baseline);
            return { ok: true };
        });

        await runStep(steps.wipe9, async () => {
            await wipeDb(DATA_DB);
            await wipeDb(PARSE_DB);
            return { ok: true };
        });

        await runStep(steps.restoreParse, async () => {
            const before = await csFetch('/api/status');
            restoreBaseline = before.lastRestore ?? null;
            await csFetch('/api/trigger/restore', {
                method: 'POST',
                body: JSON.stringify({ type: 'full', target: 'all' }),
            });
            const r = await waitForCompletion('restore', restoreBaseline);
            return { mode: r.mode };
        });

        await runStep(steps.verifyParse, async () => {
            const items = client.db(PARSE_DB).collection('items');

            // Targeted assertion: explicitly check the deleted _ids did not come back.
            // Gives a clear failure message when the bug it targets actually fires —
            // the fingerprint compare below would also catch it but with a less specific
            // "count mismatch" message.
            const stillThere = await items.find({ _id: { $in: parseDeletedIds } }).toArray();
            if (stillThere.length > 0) {
                throw new Error(
                    'Parse delete-tracking broken: ' + stillThere.length + ' of ' + parseDeletedIds.length +
                    ' deleted docs were silently restored.'
                );
            }

            // Full fingerprint compare (catches modify/insert tracking too)
            const after = await fingerprintAll();
            const issues = compareFingerprints(after, fpF, ignoreColls);
            if (issues.length) {
                throw new Error('Expected state F after parse-delete restore, got: ' + issues.slice(0, 3).join(' | '));
            }
            return { match: true, deletedDocsConfirmedAbsent: parseDeletedIds.length };
        });

        // ---- Phase N: Index preservation -----------------------------------------------
        await runStep(steps.verifyIndexes, async () => {
            // mongodump writes per-collection metadata.json containing index definitions.
            // mongorestore re-creates them. If that pipeline ever breaks, the test catches it.
            const checks = [
                { db: DATA_DB,  coll: COLLECTION_PREFIX + 'temp-1' },
                { db: DATA_DB,  coll: COLLECTION_PREFIX + 'temp-2' },
                { db: DATA_DB,  coll: CONFIG_COLLECTION },
                { db: PARSE_DB, coll: 'items' },
            ];
            const missing = [];
            for (const { db, coll } of checks) {
                const indexes = await client.db(db).collection(coll).indexes();
                const hasUpdatedAt = indexes.some(ix => ix.key && ix.key.updatedAt === 1);
                if (!hasUpdatedAt) missing.push(db + '/' + coll);
            }
            if (missing.length) {
                throw new Error(
                    'updatedAt index missing on after-restore collections: ' + missing.join(', ') +
                    '. mongodump/mongorestore index round-trip is broken.'
                );
            }
            return { collectionsChecked: checks.length, allHaveUpdatedAtIndex: true };
        });

        // ---- Phase O: Integrity verification (SHA-256 round-trip) -----------------------
        // Backups got checksums written under the new code. Verify that:
        //   1. A clean run reports every entry as ok.
        //   2. A 1-byte corruption in a real dump file is reliably detected.
        //   3. Repairing the byte returns the run to ok.
        // The corruption step uses CRASHSAFE_BACKUP_DIR (the volume mount) to
        // reach into the backup tree. testapp must NOT have this mount in
        // production setups — it's a test-harness convenience only.
        const BACKUP_DIR = process.env.CRASHSAFE_BACKUP_DIR;
        let corruptedFile = null;
        let originalLastByte = null;

        await runStep(steps.verifyCleanRun, async () => {
            const before = await csFetch('/api/status');
            const baseline = before.lastVerify ?? null;
            await csFetch('/api/trigger/verify', {
                method: 'POST',
                body: JSON.stringify({ target: 'all' }),
            });
            const r = await waitForCompletion('verify', baseline);
            // Allow 'warnings' too — old backups that pre-date this build would
            // appear as no-baseline. In a fresh local-test run there are none.
            if (r.status !== 'success' && r.status !== 'warnings') {
                throw new Error('Initial verify expected success/warnings, got: ' + r.status + ' (' + JSON.stringify(r.summary) + ')');
            }
            if ((r.summary?.corrupt ?? 0) > 0) {
                throw new Error('Initial verify reported corruption with no tampering: ' + JSON.stringify(r.summary));
            }
            return { status: r.status, summary: r.summary };
        });

        await runStep(steps.corruptBackup, async () => {
            if (!BACKUP_DIR) throw new Error('CRASHSAFE_BACKUP_DIR not set — testapp container needs the /backups volume mount for this phase');

            // Find the most recent data-DB dump file (any *.bson.gz) and flip
            // its last byte. We use the most-recent so chain-replay tests in
            // earlier phases that might re-touch older files won't drift.
            const dataRoot = path.join(BACKUP_DIR, 'data');
            if (!fs.existsSync(dataRoot)) throw new Error('Backup dir not visible at ' + dataRoot);

            // Pick the newest slug directory (slug = ISO-timestamp with -)
            const slugs = fs.readdirSync(dataRoot, { withFileTypes: true })
                .filter(e => e.isDirectory() && e.name !== 'ids')
                .map(e => e.name)
                .sort()
                .reverse();
            if (!slugs.length) throw new Error('No backup slug directories under ' + dataRoot);

            // Walk slug dir to find a .bson.gz file
            function findBson(dir) {
                for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
                    const p = path.join(dir, e.name);
                    if (e.isDirectory()) {
                        const r = findBson(p);
                        if (r) return r;
                    } else if (e.isFile() && e.name.endsWith('.bson.gz')) {
                        return p;
                    }
                }
                return null;
            }
            let target = null;
            for (const s of slugs) {
                target = findBson(path.join(dataRoot, s));
                if (target) break;
            }
            if (!target) throw new Error('No .bson.gz file found in any backup slug — nothing to corrupt');

            const buf = fs.readFileSync(target);
            originalLastByte = buf[buf.length - 1];
            buf[buf.length - 1] ^= 0xff;
            fs.writeFileSync(target, buf);
            corruptedFile = target;
            return { corrupted: path.relative(BACKUP_DIR, target) };
        });

        await runStep(steps.verifyDetectsCorruption, async () => {
            const before = await csFetch('/api/status');
            const baseline = before.lastVerify ?? null;
            await csFetch('/api/trigger/verify', {
                method: 'POST',
                body: JSON.stringify({ target: 'all' }),
            });
            const r = await waitForCompletion('verify', baseline);
            if (r.status !== 'failure') {
                throw new Error('Expected verify to detect corruption, got status=' + r.status + ' (' + JSON.stringify(r.summary) + ')');
            }
            if ((r.summary?.corrupt ?? 0) === 0) {
                throw new Error('Verify status was failure but summary.corrupt is 0 — that should not happen');
            }
            return { status: r.status, corruptEntries: r.summary.corrupt };
        });

        await runStep(steps.repairBackup, async () => {
            if (!corruptedFile || originalLastByte == null) {
                throw new Error('No corrupted file recorded; cannot repair');
            }
            const buf = fs.readFileSync(corruptedFile);
            buf[buf.length - 1] = originalLastByte;
            fs.writeFileSync(corruptedFile, buf);
            return { repaired: path.relative(BACKUP_DIR, corruptedFile) };
        });

        await runStep(steps.verifyRepaired, async () => {
            const before = await csFetch('/api/status');
            const baseline = before.lastVerify ?? null;
            await csFetch('/api/trigger/verify', {
                method: 'POST',
                body: JSON.stringify({ target: 'all' }),
            });
            const r = await waitForCompletion('verify', baseline);
            if (r.status !== 'success' && r.status !== 'warnings') {
                throw new Error('After repair, expected verify success/warnings, got: ' + r.status + ' (' + JSON.stringify(r.summary) + ')');
            }
            if ((r.summary?.corrupt ?? 0) > 0) {
                throw new Error('After repair, verify still reports corruption: ' + JSON.stringify(r.summary));
            }
            return { status: r.status, summary: r.summary };
        });

        // ---- Phase J: Manifest audit ----------------------------------------------------
        await runStep(steps.audit, async () => {
            const status = await csFetch('/api/status');
            const dataDb = status.backups?.find(b => b.type === 'data');
            const parseDb = status.backups?.find(b => b.type === 'parse');
            if (!dataDb) throw new Error('Data DB not present in status');
            if (!parseDb) throw new Error('Parse DB not present in status');
            // Expected chain: Full, Inc1, Inc2, Full2, Inc3, Inc4, Inc5, Inc6 → 8 entries each
            if (dataDb.count < 8)  throw new Error('Expected at least 8 backups in data DB, got ' + dataDb.count);
            if (parseDb.count < 8) throw new Error('Expected at least 8 backups in parse DB, got ' + parseDb.count);
            if (typeof dataDb.totalSize !== 'number' || dataDb.totalSize <= 0) {
                throw new Error('totalSize for data DB should be > 0, got ' + dataDb.totalSize);
            }
            if (typeof parseDb.totalSize !== 'number' || parseDb.totalSize <= 0) {
                throw new Error('totalSize for parse DB should be > 0, got ' + parseDb.totalSize);
            }
            // No lockfile should still be held
            if (status.inFlight) throw new Error('Lock still held after final restore — operation did not release');
            return {
                dataBackups: dataDb.count, dataSize: dataDb.totalSize,
                parseBackups: parseDb.count, parseSize: parseDb.totalSize,
            };
        });

        logEvent('success', 'All steps passed');
    } catch (err) {
        logEvent('error', 'Auto-test halted: ' + (err?.message ?? String(err)));
    } finally {
        testState.running = false;
        testState.finishedAt = new Date().toISOString();
        const total = testState.steps.length;
        const pass = testState.steps.filter(s => s.status === 'pass').length;
        const fail = testState.steps.filter(s => s.status === 'fail').length;
        const pending = testState.steps.filter(s => s.status === 'pending' || s.status === 'running').length;
        const durationMs = new Date(testState.finishedAt).getTime() - new Date(testState.startedAt).getTime();
        testState.summary = { total, pass, fail, pending, durationMs, ok: fail === 0 && pending === 0 };
    }
}

// ---------------------------------------------------------------------------
// Stress Test — configurable volume test for measuring time and resource use
// at scale. Uses count-based verification (hashing 250M docs would itself take
// hours).
// ---------------------------------------------------------------------------

const STRESS_NAME_PREFIX = COLLECTION_PREFIX + 'stress-';
const STRESS_INSERT_BATCH = 2500;
const STRESS_MAX_COLLECTIONS = 5000;
const STRESS_MAX_DOCS_PER_COLLECTION = 100000;

let stressState = null;

function makeStressDoc(collIdx, seq) {
    const now = new Date();
    return {
        sensorId: 'stress-' + collIdx,
        seq,
        value: Math.round(Math.random() * 100000) / 100,
        timestamp: now,
        updatedAt: now,
    };
}

async function stressBulkSeed(numColls, docsPerColl) {
    const total = numColls * docsPerColl;
    let inserted = 0;
    const seedStart = Date.now();

    for (let c = 0; c < numColls; c++) {
        const collName = STRESS_NAME_PREFIX + String(c).padStart(5, '0');
        const coll = client.db(DATA_DB).collection(collName);

        let collInserted = 0;
        while (collInserted < docsPerColl) {
            const thisBatch = Math.min(STRESS_INSERT_BATCH, docsPerColl - collInserted);
            const batch = [];
            for (let i = 0; i < thisBatch; i++) {
                batch.push(makeStressDoc(c, collInserted + i));
            }
            await coll.insertMany(batch, { ordered: false });
            collInserted += thisBatch;
            inserted += thisBatch;

            if (stressState) {
                const elapsedMs = Date.now() - seedStart;
                const rate = elapsedMs > 0 ? inserted / (elapsedMs / 1000) : 0;
                const remainingMs = rate > 0 ? ((total - inserted) / rate) * 1000 : null;
                stressState.progress = {
                    phase: 'seeding',
                    currentCollection: c + 1,
                    totalCollections: numColls,
                    docsInserted: inserted,
                    totalDocs: total,
                    pct: total > 0 ? Math.round((inserted / total) * 100) : 0,
                    rateDocsPerSec: Math.round(rate),
                    etaMs: remainingMs,
                };
            }
        }
    }
    return { totalDocs: inserted };
}

async function stressCountCollections(numColls) {
    const counts = {};
    for (let c = 0; c < numColls; c++) {
        const collName = STRESS_NAME_PREFIX + String(c).padStart(5, '0');
        try {
            counts[collName] = await client.db(DATA_DB).collection(collName).estimatedDocumentCount();
        } catch {
            counts[collName] = -1;
        }
    }
    return counts;
}

async function stressDropCollections(numColls) {
    const db = client.db(DATA_DB);
    const colls = await db.listCollections().toArray();
    const stressColls = colls.filter(c => c.name.startsWith(STRESS_NAME_PREFIX));
    for (const c of stressColls) {
        try { await db.collection(c.name).drop(); } catch {}
    }
    return { dropped: stressColls.length };
}

async function waitForOperationWithProgress(kind, baseline, intervalMs = 2000) {
    const start = Date.now();
    const TIMEOUT = 24 * 60 * 60 * 1000;  // 24h — stress runs can be very long
    while (Date.now() - start < TIMEOUT) {
        const status = await csFetch('/api/status');
        const inFlight = status.inFlight;

        if (stressState && inFlight) {
            if (kind === 'backup') {
                stressState.progress = {
                    phase: 'backup',
                    currentDb: inFlight.currentDb || null,
                    currentCollection: inFlight.currentCollection || null,
                    processedCollections: inFlight.processedCollections ?? 0,
                    totalCollections: inFlight.totalCollections ?? 0,
                    pct: (inFlight.totalCollections > 0)
                        ? Math.round((inFlight.processedCollections / inFlight.totalCollections) * 100) : 0,
                };
            } else {
                stressState.progress = {
                    phase: 'restore',
                    currentDb: inFlight.currentDb || null,
                    currentEntry: inFlight.currentEntry || null,
                    subPhase: inFlight.phase || null,
                    processedSteps: inFlight.processedSteps ?? 0,
                    totalSteps: inFlight.totalSteps ?? 0,
                };
            }
        }

        const r = kind === 'backup' ? (status.runs?.manual ?? null) : (status.lastRestore ?? null);
        if (!inFlight && r && r.finishedAt !== baseline?.finishedAt) {
            if (r.status !== 'success') throw new Error(kind + ' failed: ' + (r.error || r.status));
            return r;
        }
        await sleep(intervalMs);
    }
    throw new Error('Timed out waiting for ' + kind);
}

async function runStressTest(opts) {
    const numColls = Math.max(1, Math.min(parseInt(opts.collections, 10) || 100, STRESS_MAX_COLLECTIONS));
    const docsPerColl = Math.max(1, Math.min(parseInt(opts.docsPerCollection, 10) || 1000, STRESS_MAX_DOCS_PER_COLLECTION));
    const mode = ['seed-only', 'seed-and-backup', 'full-cycle'].includes(opts.mode) ? opts.mode : 'full-cycle';

    stressState = {
        running: true,
        startedAt: new Date().toISOString(),
        finishedAt: null,
        config: { numColls, docsPerColl, mode, totalDocs: numColls * docsPerColl },
        phases: [],
        progress: null,
        log: [],
        summary: null,
    };

    function logE(level, message) {
        if (!stressState) return;
        stressState.log.push({ ts: new Date().toISOString(), level, message });
        if (stressState.log.length > 300) stressState.log.shift();
    }

    function startPhase(name) {
        const p = { name, startedAt: new Date().toISOString(), finishedAt: null, durationMs: null, details: null, error: null };
        stressState.phases.push(p);
        logE('info', '▶ ' + name);
        return p;
    }
    function endPhase(p, details) {
        p.finishedAt = new Date().toISOString();
        p.durationMs = new Date(p.finishedAt).getTime() - new Date(p.startedAt).getTime();
        if (details) p.details = details;
        logE('success', '✓ ' + p.name + ' (' + (p.durationMs / 1000).toFixed(1) + 's)' + (details ? ' — ' + Object.entries(details).slice(0, 3).map(([k,v]) => k + '=' + v).join(', ') : ''));
    }
    function failPhase(p, err) {
        p.finishedAt = new Date().toISOString();
        p.durationMs = new Date(p.finishedAt).getTime() - new Date(p.startedAt).getTime();
        p.error = err?.message ?? String(err);
        logE('error', '✗ ' + p.name + ': ' + p.error);
    }

    logE('info', 'Stress test started — ' + numColls + ' collections × ' + docsPerColl + ' docs (' + (numColls * docsPerColl).toLocaleString() + ' total)');

    let preCounts;

    try {
        // Phase 1 — Clean any leftover stress collections from previous runs
        let p = startPhase('Drop existing stress collections');
        const dropped = await stressDropCollections(numColls);
        endPhase(p, { dropped: dropped.dropped });

        // Phase 2 — Bulk seed
        p = startPhase('Seed ' + numColls + ' × ' + docsPerColl + ' docs');
        const seed = await stressBulkSeed(numColls, docsPerColl);
        endPhase(p, { docs: seed.totalDocs });
        stressState.progress = null;

        // Phase 2b — Index updatedAt on every stress collection.
        // Without this, append-only mode falls back to COLLSCAN per collection on
        // every incremental, which dominates the runtime.
        p = startPhase('Create updatedAt indexes (' + numColls + ' collections)');
        const indexStart = Date.now();
        for (let c = 0; c < numColls; c++) {
            const collName = STRESS_NAME_PREFIX + String(c).padStart(5, '0');
            await ensureUpdatedAtIndex(DATA_DB, collName);
            if ((c + 1) % 100 === 0 || c === numColls - 1) {
                stressState.progress = {
                    phase: 'indexing',
                    currentCollection: c + 1,
                    totalCollections: numColls,
                    pct: Math.round(((c + 1) / numColls) * 100),
                };
            }
        }
        stressState.progress = null;
        endPhase(p, { indexed: numColls, durationMs: Date.now() - indexStart });

        // Phase 3 — Snapshot pre-backup counts
        p = startPhase('Snapshot pre-backup counts');
        preCounts = await stressCountCollections(numColls);
        const preTotal = Object.values(preCounts).reduce((a, b) => a + (b > 0 ? b : 0), 0);
        endPhase(p, { collections: numColls, totalDocs: preTotal });

        if (mode === 'seed-only') {
            logE('success', 'Stress test (seed-only) completed');
        } else {
            // Phase 4 — Trigger full backup, watch progress
            p = startPhase('Trigger full backup');
            const before = await csFetch('/api/status');
            const baseline = before.runs?.manual ?? null;
            await csFetch('/api/trigger/backup', {
                method: 'POST',
                body: JSON.stringify({ type: 'full', target: 'data' }),
            });
            await waitForOperationWithProgress('backup', baseline);
            endPhase(p);
            stressState.progress = null;

            if (mode === 'full-cycle') {
                // Phase 5 — Wipe before restore
                p = startPhase('Wipe stress collections before restore');
                const wiped = await stressDropCollections(numColls);
                endPhase(p, { dropped: wiped.dropped });

                // Phase 6 — Restore to Latest, watch progress
                p = startPhase('Restore to Latest');
                const beforeR = await csFetch('/api/status');
                const baselineR = beforeR.lastRestore ?? null;
                await csFetch('/api/trigger/restore', {
                    method: 'POST',
                    body: JSON.stringify({ type: 'full', target: 'data' }),
                });
                await waitForOperationWithProgress('restore', baselineR);
                endPhase(p);
                stressState.progress = null;

                // Phase 7 — Verify counts match
                p = startPhase('Verify post-restore counts (count-based)');
                const postCounts = await stressCountCollections(numColls);
                let mismatches = 0, missing = 0, total = 0;
                for (const coll of Object.keys(preCounts)) {
                    const expected = preCounts[coll];
                    const actual = postCounts[coll];
                    if (actual === undefined || actual < 0) missing++;
                    else if (actual !== expected) mismatches++;
                    else total += actual;
                }
                if (mismatches > 0 || missing > 0) {
                    failPhase(p, new Error(mismatches + ' count mismatches, ' + missing + ' missing collections'));
                    throw new Error('Verification failed');
                }
                endPhase(p, { verified: numColls, totalDocs: total });
            }
        }

        logE('success', 'Stress test completed successfully');
    } catch (err) {
        const last = stressState.phases[stressState.phases.length - 1];
        if (last && !last.finishedAt) failPhase(last, err);
        logE('error', 'Halted: ' + (err?.message ?? String(err)));
    } finally {
        stressState.running = false;
        stressState.finishedAt = new Date().toISOString();
        stressState.progress = null;
        const totalMs = new Date(stressState.finishedAt).getTime() - new Date(stressState.startedAt).getTime();
        const ok = stressState.phases.every(ph => !ph.error);
        stressState.summary = {
            ok,
            totalDurationMs: totalMs,
            phases: stressState.phases.map(ph => ({ name: ph.name, durationMs: ph.durationMs, error: ph.error || null, details: ph.details })),
        };
    }
}

const requestHandler = async (req, res) => {
    try {
        const u = new URL(req.url, 'http://x');

        if (req.method === 'GET' && u.pathname === '/') {
            const file = path.join(__dirname, 'public', 'index.html');
            res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
            return fs.createReadStream(file).pipe(res);
        }

        if (req.method === 'GET' && u.pathname === '/api/status') {
            return jsonResponse(res, 200, await getStatus());
        }

        if (req.method === 'GET' && u.pathname.startsWith('/api/sample/')) {
            const parts = u.pathname.split('/');
            // /api/sample/:db/:coll
            const dbName = decodeURIComponent(parts[3] || '');
            const collName = decodeURIComponent(parts[4] || '');
            const limit = u.searchParams.get('limit') || '10';
            return jsonResponse(res, 200, await getSample(dbName, collName, limit));
        }

        if (req.method === 'GET' && u.pathname === '/api/auto-test/status') {
            return jsonResponse(res, 200, testState ?? { running: false, steps: [], log: [], summary: null });
        }

        if (req.method === 'GET' && u.pathname === '/api/stress-test/status') {
            return jsonResponse(res, 200, stressState ?? { running: false, phases: [], log: [], summary: null });
        }

        if (req.method === 'POST') {
            const body = await readJsonBody(req);
            switch (u.pathname) {
                case '/api/insert':
                    return jsonResponse(res, 200, await insertDocs(body.db, body.collection, body.count));
                case '/api/modify':
                    return jsonResponse(res, 200, await modifyRandom(body.db, body.collection, body.count));
                case '/api/delete':
                    return jsonResponse(res, 200, await deleteRandom(body.db, body.collection, body.count));
                case '/api/wipe':
                    return jsonResponse(res, 200, await wipeDb(body.db));
                case '/api/drop-collection':
                    return jsonResponse(res, 200, await dropCollection(body.db, body.collection));
                case '/api/create-collection':
                    return jsonResponse(res, 200, await createCollection(body.db, body.name));
                case '/api/setup-demo':
                    return jsonResponse(res, 200, await setupDemo());
                case '/api/auto-test/start':
                    if (testState && testState.running) {
                        return jsonResponse(res, 409, { error: 'Auto-test already running' });
                    }
                    if (stressState && stressState.running) {
                        return jsonResponse(res, 409, { error: 'Stress test running — wait for it to finish' });
                    }
                    runAutoTest().catch(err => console.error('Auto-test crashed:', err));
                    return jsonResponse(res, 202, { started: true });
                case '/api/stress-test/start':
                    if (stressState && stressState.running) {
                        return jsonResponse(res, 409, { error: 'Stress test already running' });
                    }
                    if (testState && testState.running) {
                        return jsonResponse(res, 409, { error: 'Auto-test running — wait for it to finish' });
                    }
                    runStressTest({
                        collections: body.collections,
                        docsPerCollection: body.docsPerCollection,
                        mode: body.mode,
                    }).catch(err => console.error('Stress test crashed:', err));
                    return jsonResponse(res, 202, { started: true });
            }
        }

        res.writeHead(404);
        res.end('Not Found');
    } catch (err) {
        console.error('Request error:', err);
        jsonResponse(res, 500, { error: err.message ?? 'Internal error' });
    }
};

(async () => {
    await connect();
    http.createServer(requestHandler).listen(PORT, () => {
        console.log('Test harness listening on port', PORT);
    });
})().catch((err) => {
    console.error('Fatal:', err);
    process.exit(1);
});
