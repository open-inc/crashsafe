'use strict';
const fs = require('node:fs');
const path = require('node:path');
const config = require('./config');
const logger = require('./logger');

// One shared lockfile for backup AND restore — they cannot safely run at the
// same time (mongodump and mongorestore would interfere on the same DB), so a
// single mutex covers both. The `operation` field on the lockfile lets the
// dashboard distinguish between them.

const LOCKFILE_NAME = '.backup.lock';
const STALE_LOCK_THRESHOLD_MS = 24 * 60 * 60 * 1000;

let heldLockPath = null;

function lockFilePath() {
    return path.resolve(config.backupDir, LOCKFILE_NAME);
}

function isProcessAlive(pid) {
    if (!pid || typeof pid !== 'number') return false;
    try { process.kill(pid, 0); return true; }
    catch { return false; }
}

/**
 * Try to atomically acquire the cross-process lockfile.
 * @param {'backup'|'restore'} operation
 * @param {string} trigger 'scheduled' | 'api' | 'cli' | 'unknown'
 * @returns {{ ok: true } | { ok: false, holder: object|null }}
 */
function tryAcquireLock(operation, trigger) {
    const lockPath = lockFilePath();
    fs.mkdirSync(path.dirname(lockPath), { recursive: true });

    if (fs.existsSync(lockPath)) {
        let lock = null;
        try { lock = JSON.parse(fs.readFileSync(lockPath, 'utf-8')); }
        catch { /* corrupted -> treat as stale */ }
        if (lock) {
            const startedAt = new Date(lock.startedAt).getTime();
            const ageMs = Number.isFinite(startedAt) ? Date.now() - startedAt : Infinity;
            const alive = isProcessAlive(lock.pid);
            if (alive && ageMs < STALE_LOCK_THRESHOLD_MS) {
                return { ok: false, holder: lock };
            }
            logger.warn({ stalePid: lock.pid, alive, ageMs }, 'Stale operation lock detected, reclaiming');
        }
        try { fs.unlinkSync(lockPath); }
        catch (e) { if (e.code !== 'ENOENT') throw e; }
    }

    const payload = JSON.stringify({
        operation,
        pid: process.pid,
        startedAt: new Date().toISOString(),
        trigger,
    });
    try {
        // 'wx' = O_CREAT | O_EXCL: atomically fail if a concurrent acquirer wrote first
        fs.writeFileSync(lockPath, payload, { flag: 'wx', encoding: 'utf-8' });
    } catch (err) {
        if (err.code === 'EEXIST') return { ok: false, holder: null };
        throw err;
    }
    heldLockPath = lockPath;
    return { ok: true };
}

function releaseLock() {
    if (!heldLockPath) return;
    const p = heldLockPath;
    heldLockPath = null;
    try { fs.unlinkSync(p); }
    catch (err) {
        if (err.code !== 'ENOENT') {
            logger.warn({ err }, 'Failed to remove operation lock file');
        }
    }
}

/**
 * Merge progress data into the held lockfile so the dashboard can show what
 * the operation is doing. Best-effort; never throws.
 *
 * Uses write-temp + rename(2) so concurrent readers (e.g. another process
 * running tryAcquireLock) never observe a torn write.
 */
function updateLockProgress(progress) {
    if (!heldLockPath) return;
    try {
        const existing = JSON.parse(fs.readFileSync(heldLockPath, 'utf-8'));
        const merged = { ...existing, ...progress, updatedAt: new Date().toISOString() };
        const tmp = heldLockPath + '.tmp';
        fs.writeFileSync(tmp, JSON.stringify(merged), 'utf-8');
        fs.renameSync(tmp, heldLockPath);
    } catch { /* best-effort */ }
}

/** Read the current lockfile (or null if not present / corrupted). */
function readLockInfo() {
    const lockPath = lockFilePath();
    if (!fs.existsSync(lockPath)) return null;
    try { return JSON.parse(fs.readFileSync(lockPath, 'utf-8')); }
    catch { return null; }
}

// Synchronous safety net: drop the lockfile on any process exit so it isn't
// stranded for the next process even if SIGKILL / OOM hits us between writes.
process.on('exit', () => {
    if (heldLockPath) {
        try { fs.unlinkSync(heldLockPath); } catch { /* ignore */ }
    }
});

module.exports = { tryAcquireLock, releaseLock, updateLockProgress, readLockInfo };
