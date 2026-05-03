'use strict';
const { test } = require('node:test');
const assert = require('node:assert');

const { redactUri, redactErr } = require('../src/uri-redact');

test('redactUri masks password in mongodb:// URI', () => {
    const got = redactUri('mongodb://admin:hunter2@db:27017/?authSource=admin');
    assert.strictEqual(got, 'mongodb://admin:***@db:27017/?authSource=admin');
});

test('redactUri masks password in mongodb+srv:// URI', () => {
    const got = redactUri('mongodb+srv://user:s3cr3t@cluster.example.com/?retryWrites=true');
    assert.strictEqual(got, 'mongodb+srv://user:***@cluster.example.com/?retryWrites=true');
});

test('redactUri leaves URIs without auth alone', () => {
    assert.strictEqual(redactUri('mongodb://host:27017'), 'mongodb://host:27017');
    assert.strictEqual(redactUri('mongodb://user@host'), 'mongodb://user@host'); // user without password
});

test('redactUri redacts every URI in a string with multiple', () => {
    const got = redactUri('primary=mongodb://a:1@h1, secondary=mongodb://b:2@h2');
    assert.strictEqual(got, 'primary=mongodb://a:***@h1, secondary=mongodb://b:***@h2');
});

test('redactUri handles a URI embedded in a longer command line', () => {
    const cmdLike = 'mongodump --uri=mongodb://admin:p@db --db=mine --gzip';
    const got = redactUri(cmdLike);
    assert.strictEqual(got, 'mongodump --uri=mongodb://admin:***@db --db=mine --gzip');
});

test('redactUri is idempotent — running it twice does nothing extra', () => {
    const once = redactUri('mongodb://u:p@h');
    const twice = redactUri(once);
    assert.strictEqual(once, twice);
});

test('redactUri tolerates non-strings without throwing', () => {
    assert.strictEqual(redactUri(undefined), undefined);
    assert.strictEqual(redactUri(null), null);
    assert.strictEqual(redactUri(42), 42);
});

test('redactErr scrubs cmd, stderr, stdout, message and stack', () => {
    const err = new Error('mongodump failed for mongodb://admin:hunter2@db');
    err.code = 1;
    err.cmd = 'mongodump --uri=mongodb://admin:hunter2@db';
    err.stderr = 'auth failed for user admin@mongodb://admin:hunter2@db: bad creds';
    err.stdout = '';
    err.stack = err.stack + '\nat mongodb://admin:hunter2@db';

    const r = redactErr(err);

    // The password must NOT appear in any field
    const blob = JSON.stringify(r);
    assert.ok(!blob.includes('hunter2'), 'redactErr leaked the password somewhere: ' + blob);

    // But useful info is still there
    assert.strictEqual(r.code, 1);
    assert.match(r.cmd,  /mongodb:\/\/admin:\*\*\*@db/);
    assert.match(r.stderr, /mongodb:\/\/admin:\*\*\*@db/);
    assert.match(r.message, /mongodb:\/\/admin:\*\*\*@db/);
});

test('redactErr passes through non-error inputs', () => {
    assert.strictEqual(redactErr(null), null);
    assert.strictEqual(redactErr(undefined), undefined);
    assert.strictEqual(redactErr('plain string'), 'plain string');
});
