'use strict';
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { pipeline } = require('node:stream/promises');

/**
 * Compute SHA-256 of a file as a hex string. Streamed — works for files of any
 * size without loading the whole thing into memory. Refuses to follow symlinks
 * (paranoid against a manipulated backup dir trying to point at something else).
 *
 * @param {string} absPath  Absolute path to the file
 * @returns {Promise<string>}  64-char hex digest
 */
async function hashFile(absPath) {
    const lst = fs.lstatSync(absPath);
    if (lst.isSymbolicLink()) {
        throw new Error(`refusing to hash symlink: ${absPath}`);
    }
    if (!lst.isFile()) {
        throw new Error(`not a regular file: ${absPath}`);
    }
    const hash = crypto.createHash('sha256');
    await pipeline(fs.createReadStream(absPath), hash);
    return hash.digest('hex');
}

/**
 * Recursively walk `rootDir` and hash every regular file inside. Returns a map
 * from path-relative-to-`baseDir` to hex digest. Symlinks are rejected (see
 * hashFile). Walks deterministically (sorted entries) so checksum maps are
 * stable across runs.
 */
async function hashTree(rootDir, baseDir) {
    const result = {};
    async function walk(dir) {
        const entries = fs.readdirSync(dir, { withFileTypes: true })
            .sort((a, b) => a.name.localeCompare(b.name));
        for (const entry of entries) {
            const full = path.join(dir, entry.name);
            if (entry.isDirectory()) {
                await walk(full);
            } else if (entry.isFile()) {
                const rel = path.relative(baseDir, full);
                result[rel] = await hashFile(full);
            }
            // Symlinks and other non-regular file types are silently skipped —
            // hashFile would reject them, and they shouldn't appear in a normal
            // mongodump output anyway.
        }
    }
    await walk(rootDir);
    return result;
}

/**
 * Compute checksums for every file that this backup entry produced on disk.
 * Output structure splits files by class so callers can react differently:
 * a corrupt `dump` blocks any restore that would replay this entry; corrupt
 * `ids` only blocks inc-chain delete detection; corrupt `tracking` blocks
 * delete/upsert replay for this specific entry.
 *
 * Each section is null when nothing of that kind exists for this entry — e.g.
 * an inc with no changes has neither a dump dir nor a tracking file.
 */
async function computeBackupChecksums(dbBackupDir, entry) {
    const out = { dump: null, tracking: null, ids: null };

    if (entry.file) {
        const dumpDir = path.join(dbBackupDir, entry.file);
        if (fs.existsSync(dumpDir)) {
            out.dump = await hashTree(dumpDir, dbBackupDir);
        }
    }

    if (entry.trackingFile) {
        const trackingPath = path.join(dbBackupDir, entry.trackingFile);
        if (fs.existsSync(trackingPath)) {
            out.tracking = { [entry.trackingFile]: await hashFile(trackingPath) };
        }
    }

    if (entry.idDir) {
        const idsRoot = path.join(dbBackupDir, 'ids', entry.idDir);
        if (fs.existsSync(idsRoot)) {
            out.ids = await hashTree(idsRoot, dbBackupDir);
        }
    }

    return out;
}

/**
 * Resolve `relPath` against `baseDir` and assert it stays inside. Throws if
 * the path tries to escape via `..` or absolute components. Always use this
 * before opening any path read from a manifest entry — the manifest is data
 * on disk and could be tampered with.
 */
function safeJoin(baseDir, relPath) {
    const absBase = path.resolve(baseDir);
    const resolved = path.resolve(absBase, relPath);
    if (resolved !== absBase && !resolved.startsWith(absBase + path.sep)) {
        throw new Error(`path traversal blocked: ${relPath} escapes ${absBase}`);
    }
    return resolved;
}

module.exports = { hashFile, hashTree, computeBackupChecksums, safeJoin };
