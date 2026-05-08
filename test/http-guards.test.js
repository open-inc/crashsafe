'use strict';
const { test } = require('node:test');
const assert = require('node:assert');
const { Readable } = require('node:stream');

const {
    isSameOriginRequest,
    contentTypeBase,
    readBodyWithLimit,
} = require('../src/http-guards');

// ---------------------------------------------------------------------------
// isSameOriginRequest — the CSRF-gate primitive
// ---------------------------------------------------------------------------

test('isSameOriginRequest: matches Origin host with Host', () => {
    const req = { headers: { host: 'crashsafe.local:3000', origin: 'https://crashsafe.local:3000' } };
    assert.strictEqual(isSameOriginRequest(req), true);
});

test('isSameOriginRequest: rejects Origin pointing at attacker', () => {
    const req = { headers: { host: 'crashsafe.local:3000', origin: 'https://evil.example' } };
    assert.strictEqual(isSameOriginRequest(req), false);
});

test('isSameOriginRequest: rejects port mismatch (Host=:3000 vs Origin=:80)', () => {
    const req = { headers: { host: 'crashsafe.local:3000', origin: 'https://crashsafe.local' } };
    // Without explicit port in Origin, URL.host omits the default — host comparison fails.
    assert.strictEqual(isSameOriginRequest(req), false);
});

test('isSameOriginRequest: falls back to Referer when Origin is absent', () => {
    const req = { headers: { host: 'crashsafe.local:3000', referer: 'https://crashsafe.local:3000/' } };
    assert.strictEqual(isSameOriginRequest(req), true);
});

test('isSameOriginRequest: rejects Referer from another origin', () => {
    const req = { headers: { host: 'crashsafe.local:3000', referer: 'https://evil.example/foo' } };
    assert.strictEqual(isSameOriginRequest(req), false);
});

test('isSameOriginRequest: rejects when neither Origin nor Referer is set', () => {
    // This blocks form-based CSRF (browsers always send Origin on cross-origin POST).
    // Curl scripts can opt in by sending a matching Origin header.
    const req = { headers: { host: 'crashsafe.local:3000' } };
    assert.strictEqual(isSameOriginRequest(req), false);
});

test('isSameOriginRequest: rejects literal Origin: null (sandboxed iframe / file://)', () => {
    const req = { headers: { host: 'crashsafe.local:3000', origin: 'null' } };
    assert.strictEqual(isSameOriginRequest(req), false);
});

test('isSameOriginRequest: rejects when Host header is missing', () => {
    const req = { headers: { origin: 'https://crashsafe.local:3000' } };
    assert.strictEqual(isSameOriginRequest(req), false);
});

test('isSameOriginRequest: tolerates malformed Origin (URL parse error)', () => {
    const req = { headers: { host: 'crashsafe.local', origin: 'not-a-url' } };
    assert.strictEqual(isSameOriginRequest(req), false);
});

// ---------------------------------------------------------------------------
// contentTypeBase
// ---------------------------------------------------------------------------

test('contentTypeBase: strips parameters and normalises case', () => {
    assert.strictEqual(contentTypeBase({ headers: { 'content-type': 'Application/JSON; charset=utf-8' } }), 'application/json');
});

test('contentTypeBase: returns empty string when missing', () => {
    assert.strictEqual(contentTypeBase({ headers: {} }), '');
});

// ---------------------------------------------------------------------------
// readBodyWithLimit — body-size DoS gate
// ---------------------------------------------------------------------------

function streamFromChunks(chunks) {
    // Readable.from(...) returns a real stream that emits the given chunks.
    return Readable.from(chunks);
}

test('readBodyWithLimit: returns full body when under the cap', async () => {
    const body = await readBodyWithLimit(streamFromChunks(['hello ', 'world']), 1024);
    assert.strictEqual(body, 'hello world');
});

test('readBodyWithLimit: returns "" for an empty body', async () => {
    const body = await readBodyWithLimit(streamFromChunks([]), 1024);
    assert.strictEqual(body, '');
});

test('readBodyWithLimit: rejects with EBODYTOOLARGE when body exceeds the cap', async () => {
    // 2 KiB cap, 3 KiB body — should reject mid-stream.
    const big = Buffer.alloc(3 * 1024, 'a');
    await assert.rejects(
        readBodyWithLimit(streamFromChunks([big]), 2 * 1024),
        err => err && err.code === 'EBODYTOOLARGE'
    );
});

test('readBodyWithLimit: cap is exact — at the boundary it still resolves', async () => {
    const exact = Buffer.alloc(100, 'x');
    const body = await readBodyWithLimit(streamFromChunks([exact]), 100);
    assert.strictEqual(body.length, 100);
});

test('readBodyWithLimit: one byte over the boundary rejects', async () => {
    const overByOne = Buffer.alloc(101, 'x');
    await assert.rejects(
        readBodyWithLimit(streamFromChunks([overByOne]), 100),
        err => err && err.code === 'EBODYTOOLARGE'
    );
});

test('readBodyWithLimit: rejects on stream error (does not hang)', async () => {
    const bad = new Readable({ read() { this.destroy(new Error('boom')); } });
    await assert.rejects(readBodyWithLimit(bad, 1024), /boom/);
});
