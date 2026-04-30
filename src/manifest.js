'use strict';
const fs = require('node:fs');
const path = require('node:path');

const MANIFEST_FILE = 'manifest.json';

function readManifest(dbBackupDir) {
    const filePath = path.join(dbBackupDir, MANIFEST_FILE);
    if (!fs.existsSync(filePath)) return { backups: [] };
    return JSON.parse(fs.readFileSync(filePath, 'utf-8'));
}

function writeManifest(dbBackupDir, manifest) {
    fs.mkdirSync(dbBackupDir, { recursive: true });
    fs.writeFileSync(path.join(dbBackupDir, MANIFEST_FILE), JSON.stringify(manifest, null, 2), 'utf-8');
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
 * Return all entries from the first full backup up to and including backupId.
 * @param {string} dbBackupDir
 * @param {string} backupId
 * @returns {BackupEntry[]}
 */
function getChainUpTo(dbBackupDir, backupId) {
    const { backups } = readManifest(dbBackupDir);
    const idx = backups.findIndex((b) => b.id === backupId);
    if (idx === -1) return [];
    return backups.slice(0, idx + 1);
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

module.exports = { readManifest, writeManifest, appendBackupEntry, findEntry, getChainUpTo, getChainFrom };

/**
 * @typedef {object} BackupEntry
 * @property {string}   id           ISO timestamp (= the time the backup ran; used as $gt cutoff)
 * @property {'full'|'incremental'} type
 * @property {'data'|'parse'}  dbType
 * @property {string[]} collections  Collections with changes in this file
 * @property {string}   file         Dump directory name (relative to dbBackupDir)
 * @property {string}   trackingFile Tracking JSON filename (relative to dbBackupDir)
 * @property {string}   idDir        _id snapshot directory (relative to dbBackupDir/ids); contains one <collection>.jsonl per collection
 */
