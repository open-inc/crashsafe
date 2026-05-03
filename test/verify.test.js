'use strict';
const { test } = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const zlib = require('node:zlib');

const config = require('../src/config');
const { computeBackupChecksums } = require('../src/checksum');
const { runVerify, verifyEntry } = require('../src/verify');
const { writeManifest, readManifest } = require('../src/manifest');

function tmpDir() {
    return fs.mkdtempSync(path.join(os.tmpdir(), 'crashsafe-verify-'));
}

/**
 * Build a minimal-but-realistic backup directory at `<base>/<dbType>/`.
 * Returns the entry that was appended to the manifest, with its checksums
 * already computed. Mimics what backupDb does in src/backup.js.
 */
async function seedBackup(base, dbType, { id, dbName, collection, payload }) {
    const dir = path.join(base, dbType);
    const slug = id.replace(/[:.]/g, '-');
    const dumpDir = path.join(dir, slug, dbName);
    const idsDir = path.join(dir, 'ids', slug);
    fs.mkdirSync(dumpDir, { recursive: true });
    fs.mkdirSync(idsDir, { recursive: true });

    // Real-ish gzip stream so --deep tests have something it can validate.
    const gz = zlib.gzipSync(payload);
    fs.writeFileSync(path.join(dumpDir, `${collection}.bson.gz`), gz);
    fs.writeFileSync(path.join(idsDir, `${collection}.jsonl`), '"some-id"\n');

    const entry = {
        id,
        type: 'full',
        dbType,
        collections: [collection],
        file: slug,
        trackingFile: null,
        idDir: slug,
        size: 0,
        trigger: 'cli',
        finishedAt: id,
    };
    entry.checksums = await computeBackupChecksums(dir, entry);

    const m = readManifest(dir);
    m.backups.push(entry);
    writeManifest(dir, m);

    return { entry, dir, slug, dumpDir };
}

function withConfigOverride(opts, fn) {
    const restores = [];
    for (const [k, v] of Object.entries(opts)) {
        const orig = Object.getOwnPropertyDescriptor(config, k);
        Object.defineProperty(config, k, { get: () => v, configurable: true });
        restores.push(() => Object.defineProperty(config, k, orig));
    }
    return Promise.resolve(fn()).finally(() => restores.forEach(r => r()));
}

// --- verifyEntry: per-entry pure verification ------------------------------

test('verifyEntry: happy path returns ok with no issues', async () => {
    const base = tmpDir();
    try {
        const { entry, dir } = await seedBackup(base, 'data', {
            id: '2026-01-01T00:00:00.000Z', dbName: 'mydb', collection: 'col1',
            payload: 'ALPHA-BETA-GAMMA',
        });
        const r = await verifyEntry('data', dir, entry, { deep: false });
        assert.strictEqual(r.status, 'ok');
        assert.strictEqual(r.issues.length, 0);
    } finally {
        fs.rmSync(base, { recursive: true, force: true });
    }
});

test('verifyEntry: bit-flip in dump file is detected as mismatch', async () => {
    const base = tmpDir();
    try {
        const { entry, dir, dumpDir } = await seedBackup(base, 'data', {
            id: '2026-01-02T00:00:00.000Z', dbName: 'mydb', collection: 'col1',
            payload: 'WILL-BE-CORRUPTED',
        });
        // Flip the last byte of the gzip dump
        const gzPath = path.join(dumpDir, 'col1.bson.gz');
        const buf = fs.readFileSync(gzPath);
        buf[buf.length - 1] ^= 0xff;
        fs.writeFileSync(gzPath, buf);

        const r = await verifyEntry('data', dir, entry, { deep: false });
        assert.strictEqual(r.status, 'corrupt');
        const mismatch = r.issues.find(i => i.status === 'mismatch');
        assert.ok(mismatch, 'a mismatch issue should be reported');
        assert.strictEqual(mismatch.kind, 'dump');
        assert.ok(mismatch.path.endsWith('col1.bson.gz'));
        assert.ok(mismatch.expected && mismatch.actual && mismatch.expected !== mismatch.actual);
    } finally {
        fs.rmSync(base, { recursive: true, force: true });
    }
});

test('verifyEntry: deleted dump file is reported as missing', async () => {
    const base = tmpDir();
    try {
        const { entry, dir, dumpDir } = await seedBackup(base, 'data', {
            id: '2026-01-03T00:00:00.000Z', dbName: 'mydb', collection: 'col1',
            payload: 'WILL-BE-DELETED',
        });
        fs.unlinkSync(path.join(dumpDir, 'col1.bson.gz'));

        const r = await verifyEntry('data', dir, entry, { deep: false });
        assert.strictEqual(r.status, 'corrupt');
        const missing = r.issues.find(i => i.status === 'missing');
        assert.ok(missing, 'a missing issue should be reported');
        assert.ok(missing.path.endsWith('col1.bson.gz'));
    } finally {
        fs.rmSync(base, { recursive: true, force: true });
    }
});

test('verifyEntry: entry without checksums returns no-baseline', async () => {
    const base = tmpDir();
    try {
        const { entry, dir } = await seedBackup(base, 'data', {
            id: '2026-01-04T00:00:00.000Z', dbName: 'mydb', collection: 'col1',
            payload: 'LEGACY-ENTRY',
        });
        delete entry.checksums; // simulate a pre-checksum manifest entry

        const r = await verifyEntry('data', dir, entry, { deep: false });
        assert.strictEqual(r.status, 'no-baseline');
        assert.strictEqual(r.issues.length, 0);
    } finally {
        fs.rmSync(base, { recursive: true, force: true });
    }
});

test('verifyEntry: tampered manifest with path-traversal key is rejected', async () => {
    const base = tmpDir();
    try {
        const { entry, dir } = await seedBackup(base, 'data', {
            id: '2026-01-05T00:00:00.000Z', dbName: 'mydb', collection: 'col1',
            payload: 'POISONED',
        });
        // Inject a poisoned entry in the dump section that tries to escape
        entry.checksums.dump['../../etc/passwd'] = 'a'.repeat(64);

        const r = await verifyEntry('data', dir, entry, { deep: false });
        assert.strictEqual(r.status, 'corrupt');
        const unsafe = r.issues.find(i => i.status === 'unsafe-path');
        assert.ok(unsafe, 'unsafe-path issue should be reported');
        assert.ok(unsafe.path.includes('etc/passwd'));
    } finally {
        fs.rmSync(base, { recursive: true, force: true });
    }
});

// --- runVerify: integration including lock + manifest health-check ----------

test('runVerify: corrupt manifest.json surfaces as manifest-error', async () => {
    const base = tmpDir();
    try {
        // Configure crashsafe to point at our temp tree with only a "data" DB
        await withConfigOverride({
            backupDir: base,
            dbData: 'mydb',
            dbParse: null,
        }, async () => {
            // Seed a valid backup so the dir/manifest exist, then truncate the manifest
            await seedBackup(base, 'data', {
                id: '2026-01-06T00:00:00.000Z', dbName: 'mydb', collection: 'col1',
                payload: 'WILL-NUKE-MANIFEST',
            });
            fs.writeFileSync(path.join(base, 'data', 'manifest.json'), '{ this is not json');

            const result = await runVerify({ target: 'data', trigger: 'cli' });
            assert.strictEqual(result.ok, false);
            assert.strictEqual(result.summary.manifestErrors, 1);
            assert.strictEqual(result.status, 'failure');
            const err = result.details.find(d => d.status === 'manifest-error');
            assert.ok(err, 'manifest-error detail should be present');
        });
    } finally {
        fs.rmSync(base, { recursive: true, force: true });
    }
});

test('runVerify: full happy path across multiple entries returns success + ok=true', async () => {
    const base = tmpDir();
    try {
        await withConfigOverride({
            backupDir: base,
            dbData: 'mydb',
            dbParse: null,
        }, async () => {
            await seedBackup(base, 'data', {
                id: '2026-02-01T00:00:00.000Z', dbName: 'mydb', collection: 'a',
                payload: 'AAAA',
            });
            await seedBackup(base, 'data', {
                id: '2026-02-02T00:00:00.000Z', dbName: 'mydb', collection: 'b',
                payload: 'BBBB',
            });

            const result = await runVerify({ target: 'data', trigger: 'cli' });
            assert.strictEqual(result.ok, true);
            assert.strictEqual(result.status, 'success');
            assert.strictEqual(result.summary.ok, 2);
            assert.strictEqual(result.summary.corrupt, 0);
            assert.strictEqual(result.summary.noBaseline, 0);
        });
    } finally {
        fs.rmSync(base, { recursive: true, force: true });
    }
});

test('runVerify: legacy entry without checksums yields warnings status', async () => {
    const base = tmpDir();
    try {
        await withConfigOverride({
            backupDir: base,
            dbData: 'mydb',
            dbParse: null,
        }, async () => {
            const { entry } = await seedBackup(base, 'data', {
                id: '2026-02-03T00:00:00.000Z', dbName: 'mydb', collection: 'a',
                payload: 'LEGACY',
            });
            // Strip checksums and rewrite the manifest to simulate an old entry
            const manifestPath = path.join(base, 'data', 'manifest.json');
            const m = JSON.parse(fs.readFileSync(manifestPath, 'utf-8'));
            for (const b of m.backups) delete b.checksums;
            fs.writeFileSync(manifestPath, JSON.stringify(m, null, 2));

            const result = await runVerify({ target: 'data', trigger: 'cli' });
            assert.strictEqual(result.ok, true, 'no-baseline alone is not a failure');
            assert.strictEqual(result.status, 'warnings');
            assert.strictEqual(result.summary.noBaseline, 1);
            assert.strictEqual(result.summary.corrupt, 0);
            // Suppress unused-var warning
            void entry;
        });
    } finally {
        fs.rmSync(base, { recursive: true, force: true });
    }
});

// --- --deep mode -----------------------------------------------------------

test('verifyEntry deep: valid sha but truncated gzip is reported as gzip-bad', async () => {
    const base = tmpDir();
    try {
        const { entry, dir, dumpDir } = await seedBackup(base, 'data', {
            id: '2026-03-01T00:00:00.000Z', dbName: 'mydb', collection: 'col1',
            payload: 'PAYLOAD-WILL-BE-TRUNCATED',
        });
        // Truncate the gzip stream in place AND re-compute its sha so the surface
        // sha matches but the gzip content is broken — exactly the mongodump-OOM case.
        const gzPath = path.join(dumpDir, 'col1.bson.gz');
        const buf = fs.readFileSync(gzPath);
        const truncated = buf.subarray(0, Math.max(2, buf.length - 4));
        fs.writeFileSync(gzPath, truncated);
        // Recompute checksum to reflect the truncated file — surface check passes
        const fresh = await computeBackupChecksums(dir, entry);
        entry.checksums = fresh;

        // shallow: passes (sha matches)
        const shallow = await verifyEntry('data', dir, entry, { deep: false });
        assert.strictEqual(shallow.status, 'ok', 'shallow verify only checks sha');

        // deep: catches the bad gzip
        const deep = await verifyEntry('data', dir, entry, { deep: true });
        assert.strictEqual(deep.status, 'corrupt');
        const gzipBad = deep.issues.find(i => i.status === 'gzip-bad');
        assert.ok(gzipBad, 'gzip-bad issue should be reported');
        assert.ok(gzipBad.path.endsWith('col1.bson.gz'));
    } finally {
        fs.rmSync(base, { recursive: true, force: true });
    }
});
