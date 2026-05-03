'use strict';
const { test } = require('node:test');
const assert = require('node:assert');
const path = require('node:path');

// Set required env vars BEFORE requiring config — config validates lazily,
// but URI is required at first access regardless.
process.env.OPENINC_MONGO_BACKUP_URI = 'mongodb://localhost:27017';

const CONFIG_PATH = path.join(__dirname, '..', 'src', 'config.js');

function reloadConfig() {
    delete require.cache[require.resolve(CONFIG_PATH)];
    return require(CONFIG_PATH);
}

test('config refuses DB_DATA=admin', () => {
    process.env.OPENINC_MONGO_BACKUP_DB_DATA = 'admin';
    const cfg = reloadConfig();
    assert.throws(() => cfg.dbData, /system database/i);
    delete process.env.OPENINC_MONGO_BACKUP_DB_DATA;
});

test('config refuses DB_PARSE=config', () => {
    process.env.OPENINC_MONGO_BACKUP_DB_PARSE = 'config';
    const cfg = reloadConfig();
    assert.throws(() => cfg.dbParse, /system database/i);
    delete process.env.OPENINC_MONGO_BACKUP_DB_PARSE;
});

test('config refuses DB_DATA=local (case-insensitive)', () => {
    process.env.OPENINC_MONGO_BACKUP_DB_DATA = 'LOCAL';
    const cfg = reloadConfig();
    assert.throws(() => cfg.dbData, /system database/i);
    delete process.env.OPENINC_MONGO_BACKUP_DB_DATA;
});

test('config accepts a normal DB name', () => {
    process.env.OPENINC_MONGO_BACKUP_DB_DATA = 'mydata';
    const cfg = reloadConfig();
    assert.strictEqual(cfg.dbData, 'mydata');
    delete process.env.OPENINC_MONGO_BACKUP_DB_DATA;
});

test('config returns null when DB name is unset', () => {
    delete process.env.OPENINC_MONGO_BACKUP_DB_DATA;
    const cfg = reloadConfig();
    assert.strictEqual(cfg.dbData, null);
});

test('config does not block "administrator" (substring containment is not a match)', () => {
    process.env.OPENINC_MONGO_BACKUP_DB_DATA = 'administrator';
    const cfg = reloadConfig();
    assert.strictEqual(cfg.dbData, 'administrator');
    delete process.env.OPENINC_MONGO_BACKUP_DB_DATA;
});
