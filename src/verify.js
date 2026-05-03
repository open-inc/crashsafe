'use strict';
const fs = require('node:fs');
const path = require('node:path');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const execFileAsync = promisify(execFile);

const { readManifest } = require('./manifest');
const { computeBackupChecksums, safeJoin } = require('./checksum');
const { tryAcquireLock, releaseLock, updateLockProgress } = require('./locking');
const config = require('./config');
const logger = require('./logger');

// In-process mutex + last-result tracking, mirroring backup.js / restore.js so
// the dashboard renders verify uniformly.
let inFlight = null;
let lastVerify = null;

function getVerifyStats() {
    return lastVerify;
}

function dbBackupDir(dbType) {
    return path.resolve(config.backupDir, dbType);
}

function configuredDbTypes(target) {
    const types = [];
    if ((target === 'data' || target === 'all') && config.dbData) types.push('data');
    if ((target === 'parse' || target === 'all') && config.dbParse) types.push('parse');
    return types;
}

/**
 * Verify a single backup entry: re-hash everything on disk, compare against the
 * stored map. Optionally run `gunzip -t` over each *.gz dump file to catch the
 * case where the SHA matches but the gzip stream is internally inconsistent
 * (e.g. an OOM during the original mongodump truncated the deflate state).
 *
 * Returns one record:
 *   { dbType, entryId, status, issues:[ ... ] }
 * where status is 'ok' | 'corrupt' | 'no-baseline' and each issue is:
 *   { kind: 'dump'|'tracking'|'ids'|'manifest', path, status, expected?, actual?, reason? }
 */
async function verifyEntry(dbType, dir, entry, { deep }) {
    if (!entry.checksums) {
        // Entry pre-dates checksum tracking — caller decides if this is fatal.
        return { dbType, entryId: entry.id, status: 'no-baseline', issues: [] };
    }

    const issues = [];

    let actual;
    try {
        actual = await computeBackupChecksums(dir, entry);
    } catch (err) {
        return {
            dbType,
            entryId: entry.id,
            status: 'corrupt',
            issues: [{ kind: 'compute-error', path: '*', status: 'compute-error', reason: err?.message ?? String(err) }],
        };
    }

    for (const section of ['dump', 'tracking', 'ids']) {
        const expected = entry.checksums[section] || {};
        const got = actual[section] || {};
        const expKeys = new Set(Object.keys(expected));
        const gotKeys = new Set(Object.keys(got));

        for (const k of expKeys) {
            // Path-traversal guard: a tampered manifest could try to point outside dir.
            try { safeJoin(dir, k); }
            catch (err) {
                issues.push({ kind: section, path: k, status: 'unsafe-path', reason: err.message });
                continue;
            }
            if (!gotKeys.has(k)) {
                issues.push({ kind: section, path: k, status: 'missing' });
            } else if (got[k] !== expected[k]) {
                issues.push({ kind: section, path: k, status: 'mismatch', expected: expected[k], actual: got[k] });
            }
        }
        for (const k of gotKeys) {
            if (!expKeys.has(k)) {
                issues.push({ kind: section, path: k, status: 'extra' });
            }
        }
    }

    if (deep && actual.dump) {
        // gunzip -t exits non-zero on any structural problem in the deflate stream.
        // Cheap-ish but only for the *.gz files inside the dump section.
        for (const relPath of Object.keys(actual.dump)) {
            if (!relPath.endsWith('.gz')) continue;
            let absPath;
            try { absPath = safeJoin(dir, relPath); }
            catch { continue; /* already reported above */ }
            try {
                await execFileAsync('gunzip', ['-t', absPath]);
            } catch (err) {
                issues.push({
                    kind: 'dump',
                    path: relPath,
                    status: 'gzip-bad',
                    reason: (err.stderr || err.message || '').toString().trim(),
                });
            }
        }
    }

    return {
        dbType,
        entryId: entry.id,
        status: issues.length ? 'corrupt' : 'ok',
        issues,
    };
}

/**
 * Verify all (or a single) backup of the configured DBs. Holds the shared
 * backup/restore lock so the manifest and dump files don't shift under us.
 *
 * @param {object}   opts
 * @param {'data'|'parse'|'all'} [opts.target='all']
 * @param {string|null} [opts.backupId=null]  Verify only this entry's id (matched per DB)
 * @param {boolean}  [opts.deep=false]        Also run gunzip -t over each *.gz
 * @param {string}   [opts.trigger='unknown'] 'cli' | 'api' | 'scheduled'
 */
async function runVerify({ target = 'all', backupId = null, deep = false, trigger = 'unknown' } = {}) {
    if (inFlight) {
        logger.warn({ trigger }, 'Verify skipped: another verify is already running in this process');
        return { skipped: true, reason: 'in-process' };
    }

    const acquired = tryAcquireLock('verify', trigger);
    if (!acquired.ok) {
        logger.warn({ trigger, holder: acquired.holder }, 'Verify skipped: another operation holds the lock');
        return { skipped: true, reason: 'cross-process', holder: acquired.holder };
    }

    const startedAt = new Date().toISOString();
    updateLockProgress({ mode: 'verify', target, backupId: backupId ?? null, deep: !!deep });

    inFlight = (async () => {
        const summary = { ok: 0, corrupt: 0, noBaseline: 0, manifestErrors: 0 };
        const details = [];

        try {
            for (const dbType of configuredDbTypes(target)) {
                const dir = dbBackupDir(dbType);

                // Step 0: manifest health-check. A torn or missing manifest is
                // its own failure mode — surface it instead of crashing further down.
                let manifest;
                try {
                    manifest = readManifest(dir);
                } catch (err) {
                    summary.manifestErrors++;
                    details.push({
                        dbType,
                        entryId: null,
                        status: 'manifest-error',
                        issues: [{
                            kind: 'manifest',
                            path: 'manifest.json',
                            status: 'unparseable',
                            reason: err?.message ?? String(err),
                        }],
                    });
                    continue;
                }

                const entriesToVerify = backupId
                    ? manifest.backups.filter((b) => b.id === backupId)
                    : manifest.backups;

                let processed = 0;
                for (const entry of entriesToVerify) {
                    updateLockProgress({
                        currentDb: dbType,
                        currentEntry: entry.id,
                        processedEntries: processed,
                        totalEntries: entriesToVerify.length,
                    });
                    const result = await verifyEntry(dbType, dir, entry, { deep });
                    if (result.status === 'ok') summary.ok++;
                    else if (result.status === 'no-baseline') summary.noBaseline++;
                    else summary.corrupt++;
                    details.push(result);
                    processed++;
                }
            }

            // Hard failure if anything is actually corrupt or any manifest broke.
            // No-baseline entries are warnings — separate signal.
            const hasFailure = summary.corrupt > 0 || summary.manifestErrors > 0;
            const status = hasFailure
                ? 'failure'
                : (summary.noBaseline > 0 ? 'warnings' : 'success');

            lastVerify = {
                trigger,
                target,
                backupId: backupId ?? null,
                deep: !!deep,
                startedAt,
                finishedAt: new Date().toISOString(),
                status,
                summary,
            };
            return { ok: !hasFailure, status, summary, details };
        } catch (err) {
            lastVerify = {
                trigger,
                target,
                backupId: backupId ?? null,
                deep: !!deep,
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

module.exports = { runVerify, verifyEntry, getVerifyStats };
