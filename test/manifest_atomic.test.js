'use strict';
const { test } = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const { spawnSync } = require('node:child_process');

const { readManifest, writeManifest, appendBackupEntry } = require('../src/manifest');

function tmpDir() {
    return fs.mkdtempSync(path.join(os.tmpdir(), 'crashsafe-manifest-'));
}

test('round-trips a manifest correctly', () => {
    const dir = tmpDir();
    try {
        const original = {
            backups: [
                { id: '2026-01-01T00:00:00.000Z', type: 'full', dbType: 'data', collections: ['a', 'b'], size: 1234 },
                { id: '2026-01-02T00:00:00.000Z', type: 'incremental', dbType: 'data', collections: ['a'], size: 56 },
            ],
        };
        writeManifest(dir, original);
        const restored = readManifest(dir);
        assert.deepStrictEqual(restored, original);
    } finally {
        fs.rmSync(dir, { recursive: true, force: true });
    }
});

test('produces no .tmp file after a successful write', () => {
    const dir = tmpDir();
    try {
        writeManifest(dir, { backups: [] });
        const files = fs.readdirSync(dir);
        assert.deepStrictEqual(files.sort(), ['manifest.json']);
    } finally {
        fs.rmSync(dir, { recursive: true, force: true });
    }
});

test('orphan .tmp file from a previous crash does not affect reads', () => {
    const dir = tmpDir();
    try {
        // Real, valid manifest
        writeManifest(dir, { backups: [{ id: 'good', type: 'full' }] });
        // Simulate a crash that left half-written garbage in the tmp file
        fs.writeFileSync(path.join(dir, 'manifest.json.tmp'), '{"backups":[{"id":"BROKEN', 'utf-8');

        // readManifest must read the real manifest, not the tmp
        const m = readManifest(dir);
        assert.strictEqual(m.backups.length, 1);
        assert.strictEqual(m.backups[0].id, 'good');
    } finally {
        fs.rmSync(dir, { recursive: true, force: true });
    }
});

test('next write cleans up an orphan .tmp file', () => {
    const dir = tmpDir();
    try {
        writeManifest(dir, { backups: [] });
        fs.writeFileSync(path.join(dir, 'manifest.json.tmp'), 'garbage', 'utf-8');

        // The next write should overwrite the tmp and rename it onto target.
        // Result: no orphan tmp left behind.
        writeManifest(dir, { backups: [{ id: 'x', type: 'full' }] });
        const files = fs.readdirSync(dir);
        assert.deepStrictEqual(files.sort(), ['manifest.json']);

        const m = readManifest(dir);
        assert.strictEqual(m.backups[0].id, 'x');
    } finally {
        fs.rmSync(dir, { recursive: true, force: true });
    }
});

test('100 sequential writes never produce a torn manifest', () => {
    const dir = tmpDir();
    try {
        for (let i = 0; i < 100; i++) {
            appendBackupEntry(dir, { id: 'entry-' + i, type: i === 0 ? 'full' : 'incremental' });
            // Read after every write — must always parse cleanly
            const m = readManifest(dir);
            assert.strictEqual(m.backups.length, i + 1);
            assert.strictEqual(m.backups[i].id, 'entry-' + i);
        }
    } finally {
        fs.rmSync(dir, { recursive: true, force: true });
    }
});

// --- Crash-simulation test --------------------------------------------------
// Spawn a child that races a SIGKILL against an in-progress writeManifest.
// Whatever the timing, the on-disk manifest must always be valid JSON. This is
// the test the OLD writeFileSync implementation would have failed eventually:
// a SIGKILL between the open() and the final byte of the write leaves a
// truncated manifest.json — exactly the failure mode that takes the daemon
// out at the next start.

const CHILD_SCRIPT = `
'use strict';
const { writeManifest } = require(${JSON.stringify(path.join(__dirname, '..', 'src', 'manifest.js'))});
// With node -e CODE arg, process.argv is [node, arg] — no [eval] placeholder.
const dir = process.argv[1];
// Generous size so the writeFileSync syscall has many bytes to flush — that
// widens the window where SIGKILL can land mid-write.
const big = { backups: [] };
for (let i = 0; i < 5000; i++) {
    big.backups.push({
        id: '2026-01-01T00:00:' + String(i).padStart(2, '0') + '.000Z',
        type: i % 50 === 0 ? 'full' : 'incremental',
        dbType: 'data',
        collections: ['sensors---a', 'sensors---b', 'sensors---c'],
        size: 1234567,
        trigger: 'scheduled',
        finishedAt: '2026-01-01T00:00:00.000Z',
    });
}
// Loop forever writing — parent will SIGKILL at a random point.
while (true) writeManifest(dir, big);
`;

test('SIGKILL during writeManifest leaves a valid manifest', () => {
    const dir = tmpDir();
    try {
        // Seed a known-good manifest so we always have a valid baseline on disk
        writeManifest(dir, { backups: [{ id: 'seed', type: 'full' }] });

        // Track outcomes so we can detect a degenerate run where the child
        // got SIGKILL'd before ever writing once (the test would then pass
        // trivially without actually exercising the torn-write window).
        let bigStateSeen = 0;
        const trials = 12;
        for (let i = 0; i < trials; i++) {
            // 100–400ms: on a cold start the child needs ~50ms to load Node
            // and another ~30ms to build the 5000-entry payload. Anything
            // shorter risks killing during startup before writeManifest is
            // even reached. The upper end is wide enough to occasionally land
            // mid-rename, which is the actually-interesting case.
            const killDelay = 100 + Math.floor(Math.random() * 300);
            spawnSync(process.execPath, ['-e', CHILD_SCRIPT, dir], {
                stdio: 'ignore',
                timeout: killDelay,
                killSignal: 'SIGKILL',
            });

            // After SIGKILL: manifest.json MUST parse and MUST contain at
            // least one entry (seed or big-write state). Torn JSON would
            // throw inside JSON.parse here — exactly the failure the old
            // non-atomic writeFileSync produces.
            const m = readManifest(dir);
            assert.ok(Array.isArray(m.backups), `trial ${i}: manifest.backups is not an array after SIGKILL`);
            assert.ok(m.backups.length > 0, `trial ${i}: manifest.backups is empty after SIGKILL`);
            if (m.backups.length > 1) bigStateSeen++;
        }

        // Sanity: at least some trials must have actually reached the write
        // loop, otherwise the test passed trivially without exercising the
        // race window.
        assert.ok(
            bigStateSeen > 0,
            'No trial ever observed the big-write state — SIGKILL always landed during startup. Increase killDelay range.'
        );
    } finally {
        fs.rmSync(dir, { recursive: true, force: true });
    }
});
