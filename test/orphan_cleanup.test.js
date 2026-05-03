'use strict';
const { test } = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');

const config = require('../src/config');
const { cleanupOrphans } = require('../src/backup');
const { writeManifest } = require('../src/manifest');

function tmpDir() {
    return fs.mkdtempSync(path.join(os.tmpdir(), 'crashsafe-orphan-'));
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

/**
 * Synthesize the on-disk shape backupDb produces, without running mongodump.
 * Creates `<base>/<dbType>/<slug>/<dbName>/file.bson.gz`,
 * `<base>/<dbType>/<slug>.tracking.json`, and `<base>/<dbType>/ids/<slug>/<col>.jsonl`.
 */
function seedSlug(base, dbType, slug, { dbName = 'mydb', withTracking = true } = {}) {
    const dir = path.join(base, dbType);
    fs.mkdirSync(path.join(dir, slug, dbName), { recursive: true });
    fs.mkdirSync(path.join(dir, 'ids', slug), { recursive: true });
    fs.writeFileSync(path.join(dir, slug, dbName, 'col1.bson.gz'), 'fakedump');
    fs.writeFileSync(path.join(dir, 'ids', slug, 'col1.jsonl'), '"x"\n');
    if (withTracking) {
        fs.writeFileSync(path.join(dir, slug + '.tracking.json'), '[]');
    }
}

// All-in-the-past slug strings so they pass the 1h safety threshold.
const OLD_SLUG_KNOWN  = '2020-01-01T00-00-00-000Z';
const OLD_SLUG_ORPHAN = '2020-01-02T00-00-00-000Z';

test('cleanupOrphans deletes slug dirs not in manifest, keeps known ones', async () => {
    const base = tmpDir();
    try {
        await withConfigOverride({
            backupDir: base,
            dbData: 'mydb',
            dbParse: null,
        }, async () => {
            seedSlug(base, 'data', OLD_SLUG_KNOWN);
            seedSlug(base, 'data', OLD_SLUG_ORPHAN);

            // Manifest only mentions the "known" slug
            writeManifest(path.join(base, 'data'), {
                backups: [{
                    id: '2020-01-01T00:00:00.000Z',
                    type: 'full',
                    dbType: 'data',
                    collections: ['col1'],
                    file: OLD_SLUG_KNOWN,
                    trackingFile: OLD_SLUG_KNOWN + '.tracking.json',
                    idDir: OLD_SLUG_KNOWN,
                }],
            });

            cleanupOrphans();

            // Known stays
            assert.ok(fs.existsSync(path.join(base, 'data', OLD_SLUG_KNOWN)),  'known dump dir must be preserved');
            assert.ok(fs.existsSync(path.join(base, 'data', 'ids', OLD_SLUG_KNOWN)),  'known ids dir must be preserved');
            assert.ok(fs.existsSync(path.join(base, 'data', OLD_SLUG_KNOWN + '.tracking.json')), 'known tracking file must be preserved');

            // Orphan goes
            assert.ok(!fs.existsSync(path.join(base, 'data', OLD_SLUG_ORPHAN)),  'orphan dump dir must be deleted');
            assert.ok(!fs.existsSync(path.join(base, 'data', 'ids', OLD_SLUG_ORPHAN)),  'orphan ids dir must be deleted');
            assert.ok(!fs.existsSync(path.join(base, 'data', OLD_SLUG_ORPHAN + '.tracking.json')), 'orphan tracking file must be deleted');
        });
    } finally {
        fs.rmSync(base, { recursive: true, force: true });
    }
});

test('cleanupOrphans does NOT delete recent slug dirs (safety threshold)', async () => {
    const base = tmpDir();
    try {
        await withConfigOverride({
            backupDir: base,
            dbData: 'mydb',
            dbParse: null,
        }, async () => {
            // Slug timestamp = now → too young to clean even though it's not in manifest
            const recentSlug = new Date().toISOString().replace(/[:.]/g, '-');
            seedSlug(base, 'data', recentSlug);

            writeManifest(path.join(base, 'data'), { backups: [] });

            cleanupOrphans();

            assert.ok(fs.existsSync(path.join(base, 'data', recentSlug)),
                'recent orphan dump dir must NOT be deleted (could still be in-flight)');
            assert.ok(fs.existsSync(path.join(base, 'data', recentSlug + '.tracking.json')),
                'recent orphan tracking file must NOT be deleted');
            assert.ok(fs.existsSync(path.join(base, 'data', 'ids', recentSlug)),
                'recent orphan ids dir must NOT be deleted');
        });
    } finally {
        fs.rmSync(base, { recursive: true, force: true });
    }
});

test('cleanupOrphans leaves non-slug paths untouched', async () => {
    const base = tmpDir();
    try {
        await withConfigOverride({
            backupDir: base,
            dbData: 'mydb',
            dbParse: null,
        }, async () => {
            const dir = path.join(base, 'data');
            fs.mkdirSync(dir, { recursive: true });

            // Files / dirs that aren't slug-shaped — must all survive
            fs.writeFileSync(path.join(dir, 'manifest.json'), JSON.stringify({ backups: [] }));
            fs.writeFileSync(path.join(dir, 'README.txt'), 'operator note: do not delete');
            fs.mkdirSync(path.join(dir, 'random-folder'), { recursive: true });
            fs.writeFileSync(path.join(dir, 'random-folder', 'stuff'), 'x');

            cleanupOrphans();

            assert.ok(fs.existsSync(path.join(dir, 'manifest.json')));
            assert.ok(fs.existsSync(path.join(dir, 'README.txt')));
            assert.ok(fs.existsSync(path.join(dir, 'random-folder', 'stuff')));
        });
    } finally {
        fs.rmSync(base, { recursive: true, force: true });
    }
});

test('cleanupOrphans tolerates missing manifest gracefully', async () => {
    const base = tmpDir();
    try {
        await withConfigOverride({
            backupDir: base,
            dbData: 'mydb',
            dbParse: null,
        }, async () => {
            // Write nothing — no manifest, no slug dirs, no nothing.
            // cleanupOrphans must not throw.
            assert.doesNotThrow(() => cleanupOrphans());
        });
    } finally {
        fs.rmSync(base, { recursive: true, force: true });
    }
});

test('cleanupOrphans handles both data and parse DBs independently', async () => {
    const base = tmpDir();
    try {
        await withConfigOverride({
            backupDir: base,
            dbData: 'mydb',
            dbParse: 'parsedb',
        }, async () => {
            seedSlug(base, 'data',  OLD_SLUG_ORPHAN);
            seedSlug(base, 'parse', OLD_SLUG_ORPHAN);
            writeManifest(path.join(base, 'data'),  { backups: [] });
            writeManifest(path.join(base, 'parse'), { backups: [] });

            cleanupOrphans();

            assert.ok(!fs.existsSync(path.join(base, 'data',  OLD_SLUG_ORPHAN)));
            assert.ok(!fs.existsSync(path.join(base, 'parse', OLD_SLUG_ORPHAN)));
        });
    } finally {
        fs.rmSync(base, { recursive: true, force: true });
    }
});
