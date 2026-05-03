'use strict';
const fs = require('node:fs');
const path = require('node:path');

const MANIFEST_FILE = 'manifest.json';

/**
 * Recursively sum the size in bytes of every file under `dir`.
 * Returns 0 if the directory does not exist.
 */
function dirSize(dir) {
    if (!fs.existsSync(dir)) return 0;
    let total = 0;
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
        const p = path.join(dir, entry.name);
        if (entry.isDirectory()) {
            total += dirSize(p);
        } else if (entry.isFile()) {
            try { total += fs.statSync(p).size; } catch { /* file gone — ignore */ }
        }
    }
    return total;
}

/**
 * Total bytes a single backup occupies on disk: dump dir + tracking file + ids
 * (covers both the new per-collection JSONL directory and the legacy single-file
 * snapshot, so backfill works on entries written by older versions too).
 */
function computeBackupSize(dbBackupDir, slug) {
    let total = 0;
    total += dirSize(path.join(dbBackupDir, slug));

    const trackingFile = path.join(dbBackupDir, `${slug}.tracking.json`);
    if (fs.existsSync(trackingFile)) {
        try { total += fs.statSync(trackingFile).size; } catch { /* ignore */ }
    }

    // New format: ids/<slug>/<col>.jsonl directory.
    total += dirSize(path.join(dbBackupDir, 'ids', slug));

    // Legacy format: ids/<slug>.json single file.
    const legacyIds = path.join(dbBackupDir, 'ids', `${slug}.json`);
    if (fs.existsSync(legacyIds)) {
        try { total += fs.statSync(legacyIds).size; } catch { /* ignore */ }
    }

    return total;
}

function readManifest(dbBackupDir) {
    const filePath = path.join(dbBackupDir, MANIFEST_FILE);
    if (!fs.existsSync(filePath)) return { backups: [] };
    return JSON.parse(fs.readFileSync(filePath, 'utf-8'));
}

/**
 * Atomically write `content` to `filePath`. Crash-safe in three ways:
 *   1. Write to a sibling `.tmp` file, then rename(2) onto the target.
 *      POSIX rename within one directory is atomic — concurrent readers always
 *      see either the previous file or the new one, never a torn write.
 *   2. fsync the tmp file before the rename, so the data has reached disk and
 *      can't be lost to a power cut between the rename and the page-cache flush.
 *   3. fsync the parent directory after the rename, so the directory entry
 *      change itself is durable. Without this, a crash could undo the rename
 *      even though the new file's data was already on disk.
 *
 * Best-effort: directory fsync is wrapped in try/catch because it's not
 * supported on every platform (Windows). On the Linux container this code
 * runs in, all four steps execute.
 */
function atomicWriteFile(filePath, content) {
    const dir = path.dirname(filePath);
    const tmpPath = filePath + '.tmp';

    fs.writeFileSync(tmpPath, content, 'utf-8');

    // fsync the tmp file's data to disk before swapping it into place
    const fd = fs.openSync(tmpPath, 'r');
    try { fs.fsyncSync(fd); }
    finally { fs.closeSync(fd); }

    fs.renameSync(tmpPath, filePath);

    // fsync the directory so the rename itself is durable
    try {
        const dirFd = fs.openSync(dir, 'r');
        try { fs.fsyncSync(dirFd); }
        finally { fs.closeSync(dirFd); }
    } catch { /* directory fsync not supported on this platform */ }
}

function writeManifest(dbBackupDir, manifest) {
    fs.mkdirSync(dbBackupDir, { recursive: true });
    atomicWriteFile(
        path.join(dbBackupDir, MANIFEST_FILE),
        JSON.stringify(manifest, null, 2)
    );
}

function appendBackupEntry(dbBackupDir, entry) {
    const manifest = readManifest(dbBackupDir);
    manifest.backups.push(entry);
    writeManifest(dbBackupDir, manifest);
}

/**
 * Find a backup entry by ID, or return the latest entry if backupId is null.
 * @param {string} dbBackupDir
 * @param {string|null} backupId
 * @returns {BackupEntry|null}
 */
function findEntry(dbBackupDir, backupId) {
    const { backups } = readManifest(dbBackupDir);
    if (!backups.length) return null;
    if (!backupId) return backups[backups.length - 1];
    return backups.find((b) => b.id === backupId) ?? null;
}

/**
 * Return the replay chain ending at backupId. The chain starts at the most
 * recent `type:'full'` entry at-or-before backupId, because each full backup
 * is a checkpoint — anything before it is irrelevant for a wipe-and-restore.
 *
 * Without this, an empty DB captured as a fresh full would be undone by the
 * earlier full's data being replayed first (the empty full has no dump dir
 * and no tracking, so it can't actively re-empty the database).
 *
 * @param {string} dbBackupDir
 * @param {string} backupId
 * @returns {BackupEntry[]}
 */
function getChainUpTo(dbBackupDir, backupId) {
    const { backups } = readManifest(dbBackupDir);
    const idx = backups.findIndex((b) => b.id === backupId);
    if (idx === -1) return [];
    let startIdx = idx;
    while (startIdx > 0 && backups[startIdx].type !== 'full') startIdx--;
    return backups.slice(startIdx, idx + 1);
}

/**
 * Return all entries starting from sinceId (inclusive) up to toId (inclusive).
 * If toId is null, returns everything from sinceId to the latest entry.
 * @param {string} dbBackupDir
 * @param {string} sinceId
 * @param {string|null} toId
 * @returns {BackupEntry[]}
 */
function getChainFrom(dbBackupDir, sinceId, toId = null) {
    const { backups } = readManifest(dbBackupDir);
    const fromIdx = backups.findIndex((b) => b.id === sinceId);
    if (fromIdx === -1) return [];
    const toIdx = toId ? backups.findIndex((b) => b.id === toId) : backups.length - 1;
    if (toIdx === -1 || toIdx < fromIdx) return [];
    return backups.slice(fromIdx, toIdx + 1);
}

module.exports = { readManifest, writeManifest, appendBackupEntry, findEntry, getChainUpTo, getChainFrom, dirSize, computeBackupSize };

/**
 * @typedef {object} BackupEntry
 * @property {string}   id           ISO timestamp (= the time the backup ran; used as $gt cutoff)
 * @property {'full'|'incremental'} type
 * @property {'data'|'parse'}  dbType
 * @property {string[]} collections  Collections with changes in this file
 * @property {string}   file         Dump directory name (relative to dbBackupDir)
 * @property {string}   trackingFile Tracking JSON filename (relative to dbBackupDir)
 * @property {string}   idDir        _id snapshot directory (relative to dbBackupDir/ids); contains one <collection>.jsonl per collection
 * @property {number}   [size]       Total disk usage of this backup in bytes (dump + tracking + id snapshot). Optional for entries written before size tracking existed; backfilled lazily on daemon start.
 * @property {string}   [trigger]    Origin of the run that wrote this entry: 'scheduled' | 'api' | 'cli'. Optional for entries written before trigger persistence existed.
 * @property {string}   [finishedAt] ISO timestamp when this DB's portion of the run completed. Optional for entries written before this field existed.
 * @property {object}   [checksums]  SHA-256 hex digests of every file produced by this backup, keyed by path relative to dbBackupDir. Split into three sections so verify failures can be triaged: `dump` corrupt blocks restore; `tracking` corrupt blocks delete/upsert replay; `ids` corrupt blocks inc-chain delete detection. Each section is null when no file of that kind exists for the entry. Optional for entries written before checksum tracking existed; verify reports those as 'no-baseline' rather than failing.
 * @property {object}   [checksums.dump]      Map of `<file dump path relative to dbBackupDir>` → hex sha256
 * @property {object}   [checksums.tracking]  Map containing the single tracking-file path → hex sha256
 * @property {object}   [checksums.ids]       Map of `<id-snapshot path relative to dbBackupDir>` → hex sha256
 */
