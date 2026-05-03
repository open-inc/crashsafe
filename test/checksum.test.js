'use strict';
const { test } = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const crypto = require('node:crypto');

const { hashFile, hashTree, computeBackupChecksums, safeJoin } = require('../src/checksum');

function tmpDir() {
    return fs.mkdtempSync(path.join(os.tmpdir(), 'crashsafe-checksum-'));
}

function syncSha256Hex(buf) {
    return crypto.createHash('sha256').update(buf).digest('hex');
}

test('hashFile matches readFileSync hash for a small file', async () => {
    const dir = tmpDir();
    try {
        const p = path.join(dir, 'small.bin');
        const data = Buffer.from('hello world ' + 'x'.repeat(100));
        fs.writeFileSync(p, data);
        const got = await hashFile(p);
        const want = syncSha256Hex(data);
        assert.strictEqual(got, want);
    } finally {
        fs.rmSync(dir, { recursive: true, force: true });
    }
});

test('hashFile of an empty file is the well-known empty SHA-256', async () => {
    const dir = tmpDir();
    try {
        const p = path.join(dir, 'empty.bin');
        fs.writeFileSync(p, '');
        const got = await hashFile(p);
        // SHA-256 of zero bytes
        assert.strictEqual(got, 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855');
    } finally {
        fs.rmSync(dir, { recursive: true, force: true });
    }
});

test('hashFile streams a >16 MiB file correctly (would OOM if loaded into memory carelessly)', async () => {
    const dir = tmpDir();
    try {
        const p = path.join(dir, 'big.bin');
        // 20 MiB of pseudo-random bytes
        const chunkSize = 1 << 20; // 1 MiB
        const chunks = 20;
        const want = crypto.createHash('sha256');
        const fd = fs.openSync(p, 'w');
        try {
            for (let i = 0; i < chunks; i++) {
                const buf = crypto.randomBytes(chunkSize);
                fs.writeSync(fd, buf);
                want.update(buf);
            }
        } finally {
            fs.closeSync(fd);
        }
        const got = await hashFile(p);
        assert.strictEqual(got, want.digest('hex'));
    } finally {
        fs.rmSync(dir, { recursive: true, force: true });
    }
});

test('hashFile throws on missing file', async () => {
    await assert.rejects(
        () => hashFile('/nonexistent/path/to/nothing.bin'),
        /ENOENT|no such file/i,
    );
});

test('hashFile refuses symlinks', async () => {
    const dir = tmpDir();
    try {
        const real = path.join(dir, 'real.bin');
        fs.writeFileSync(real, 'real');
        const link = path.join(dir, 'link.bin');
        fs.symlinkSync(real, link);
        await assert.rejects(() => hashFile(link), /symlink/);
    } finally {
        fs.rmSync(dir, { recursive: true, force: true });
    }
});

test('hashTree returns relative paths and is order-independent (sorted)', async () => {
    const dir = tmpDir();
    try {
        // Build a tree with a deterministic content
        const sub = path.join(dir, 'subdir');
        fs.mkdirSync(sub);
        fs.writeFileSync(path.join(dir, 'b.txt'), 'BBBB');
        fs.writeFileSync(path.join(dir, 'a.txt'), 'AAAA');
        fs.writeFileSync(path.join(sub, 'c.txt'), 'CCCC');

        const map = await hashTree(dir, dir);
        const keys = Object.keys(map);
        assert.deepStrictEqual(keys.sort(), ['a.txt', 'b.txt', 'subdir/c.txt'].map(p => p.split('/').join(path.sep)));
        assert.strictEqual(map[keys.find(k => k.endsWith('a.txt'))], syncSha256Hex(Buffer.from('AAAA')));
    } finally {
        fs.rmSync(dir, { recursive: true, force: true });
    }
});

test('computeBackupChecksums splits dump/tracking/ids correctly', async () => {
    const dir = tmpDir();
    try {
        // Synthesize a backup-like layout under <dir>/<dbType>/
        const dbType = 'data';
        const base = path.join(dir, dbType);
        const slug = '2026-05-01T00-00-00-000Z';
        const dumpDir = path.join(base, slug, 'mydb');
        const idsDir = path.join(base, 'ids', slug);
        fs.mkdirSync(dumpDir, { recursive: true });
        fs.mkdirSync(idsDir, { recursive: true });
        fs.writeFileSync(path.join(dumpDir, 'col1.bson.gz'), 'BSONDATA');
        fs.writeFileSync(path.join(dumpDir, 'col1.metadata.json.gz'), 'METADATA');
        fs.writeFileSync(path.join(idsDir, 'col1.jsonl'), '{"_id":"x"}\n');
        fs.writeFileSync(path.join(base, slug + '.tracking.json'), '[{"op":"track"}]');

        const entry = {
            id: '2026-05-01T00:00:00.000Z',
            file: slug,
            trackingFile: slug + '.tracking.json',
            idDir: slug,
        };
        const out = await computeBackupChecksums(base, entry);

        assert.ok(out.dump, 'dump section present');
        const dumpKeys = Object.keys(out.dump).sort();
        assert.strictEqual(dumpKeys.length, 2);
        assert.ok(dumpKeys[0].endsWith('col1.bson.gz') || dumpKeys[1].endsWith('col1.bson.gz'));

        assert.ok(out.tracking, 'tracking section present');
        assert.ok(out.tracking[slug + '.tracking.json']);

        assert.ok(out.ids, 'ids section present');
        const idsKeys = Object.keys(out.ids);
        assert.strictEqual(idsKeys.length, 1);
        assert.ok(idsKeys[0].endsWith('col1.jsonl'));
    } finally {
        fs.rmSync(dir, { recursive: true, force: true });
    }
});

test('computeBackupChecksums returns null sections when files do not exist', async () => {
    const dir = tmpDir();
    try {
        const entry = { id: 'x', file: null, trackingFile: null, idDir: null };
        const out = await computeBackupChecksums(dir, entry);
        assert.strictEqual(out.dump, null);
        assert.strictEqual(out.tracking, null);
        assert.strictEqual(out.ids, null);
    } finally {
        fs.rmSync(dir, { recursive: true, force: true });
    }
});

test('safeJoin allows valid relative paths', () => {
    const base = '/tmp/backup';
    assert.strictEqual(safeJoin(base, 'foo/bar.bin'), path.resolve(base, 'foo/bar.bin'));
    assert.strictEqual(safeJoin(base, 'a/b/c'), path.resolve(base, 'a/b/c'));
});

test('safeJoin blocks ../ traversal', () => {
    assert.throws(() => safeJoin('/tmp/backup', '../etc/passwd'), /path traversal/);
    assert.throws(() => safeJoin('/tmp/backup', 'foo/../../etc/passwd'), /path traversal/);
});

test('safeJoin blocks absolute paths that escape', () => {
    assert.throws(() => safeJoin('/tmp/backup', '/etc/passwd'), /path traversal/);
});

test('safeJoin allows the base dir itself (rare but legitimate)', () => {
    const base = '/tmp/backup';
    assert.strictEqual(safeJoin(base, '.'), path.resolve(base));
});
