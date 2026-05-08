'use strict';
const { test } = require('node:test');
const assert = require('node:assert');

const { parseAllowList, isAllowed, getClientIp, ipToBytes } = require('../src/ip-allow');

test('parseAllowList: empty / falsy input returns an empty list', () => {
    assert.deepStrictEqual(parseAllowList(''), []);
    assert.deepStrictEqual(parseAllowList(null), []);
    assert.deepStrictEqual(parseAllowList(undefined), []);
});

test('parseAllowList: trims whitespace and skips blank entries', () => {
    const list = parseAllowList(' 10.0.0.1 , , 192.168.0.0/16 ');
    assert.strictEqual(list.length, 2);
});

test('parseAllowList: throws on malformed entries (loud-fail at startup)', () => {
    assert.throws(() => parseAllowList('not-an-ip'), /Invalid IP/);
    assert.throws(() => parseAllowList('10.0.0.1/33'), /Invalid IP/);
    assert.throws(() => parseAllowList('10.0.0.1/-1'), /Invalid IP/);
    assert.throws(() => parseAllowList('10.0.0.300'), /Invalid IP/);
    assert.throws(() => parseAllowList('::ffff:1.2.3.4/130'), /Invalid IP/);
});

test('isAllowed: empty list = allow everything (preserves existing behavior)', () => {
    assert.strictEqual(isAllowed('1.2.3.4', []), true);
    assert.strictEqual(isAllowed('::1', []), true);
});

test('isAllowed: single IPv4 match', () => {
    const list = parseAllowList('192.168.1.5');
    assert.strictEqual(isAllowed('192.168.1.5', list), true);
    assert.strictEqual(isAllowed('192.168.1.6', list), false);
});

test('isAllowed: IPv4 CIDR match', () => {
    const list = parseAllowList('10.0.0.0/8');
    assert.strictEqual(isAllowed('10.0.0.1', list), true);
    assert.strictEqual(isAllowed('10.255.255.255', list), true);
    assert.strictEqual(isAllowed('11.0.0.1', list), false);
    assert.strictEqual(isAllowed('9.255.255.255', list), false);
});

test('isAllowed: IPv4 /24 boundary correctness', () => {
    const list = parseAllowList('192.168.1.0/24');
    assert.strictEqual(isAllowed('192.168.1.0', list), true);
    assert.strictEqual(isAllowed('192.168.1.255', list), true);
    assert.strictEqual(isAllowed('192.168.0.255', list), false);
    assert.strictEqual(isAllowed('192.168.2.0', list), false);
});

test('isAllowed: non-byte-aligned prefix (/12)', () => {
    // 172.16.0.0/12 covers 172.16.0.0 – 172.31.255.255
    const list = parseAllowList('172.16.0.0/12');
    assert.strictEqual(isAllowed('172.16.0.1', list), true);
    assert.strictEqual(isAllowed('172.31.255.255', list), true);
    assert.strictEqual(isAllowed('172.32.0.0', list), false);
    assert.strictEqual(isAllowed('172.15.255.255', list), false);
});

test('isAllowed: IPv6 single + CIDR', () => {
    const list = parseAllowList('::1, 2001:db8::/32');
    assert.strictEqual(isAllowed('::1', list), true);
    assert.strictEqual(isAllowed('2001:db8::1', list), true);
    assert.strictEqual(isAllowed('2001:db8:abcd::1', list), true);
    assert.strictEqual(isAllowed('2001:db9::1', list), false);
});

test('isAllowed: IPv4-mapped IPv6 (::ffff:x.y.z.w) is treated as IPv4', () => {
    // Critical: Node binds to :: by default and reports IPv4 peers in this form.
    // Without normalization, the operator's "10.0.0.0/8" rule would silently miss them.
    const list = parseAllowList('10.0.0.0/8');
    assert.strictEqual(isAllowed('::ffff:10.1.2.3', list), true);
    assert.strictEqual(isAllowed('::ffff:11.1.2.3', list), false);
});

test('isAllowed: rejects garbage IPs (no implicit allow)', () => {
    const list = parseAllowList('10.0.0.0/8');
    assert.strictEqual(isAllowed('not-an-ip', list), false);
    assert.strictEqual(isAllowed('', list), false);
    assert.strictEqual(isAllowed(undefined, list), false);
});

test('isAllowed: IPv4 entry does not match unrelated IPv6', () => {
    const list = parseAllowList('10.0.0.0/8');
    assert.strictEqual(isAllowed('::1', list), false);
    assert.strictEqual(isAllowed('2001:db8::1', list), false);
});

test('isAllowed: /0 matches every address of the same family', () => {
    const v4 = parseAllowList('0.0.0.0/0');
    assert.strictEqual(isAllowed('1.2.3.4', v4), true);
    assert.strictEqual(isAllowed('255.255.255.255', v4), true);
    // /0 IPv4 does NOT match IPv6 addresses.
    assert.strictEqual(isAllowed('2001:db8::1', v4), false);
});

test('ipToBytes: strips IPv6 zone identifier', () => {
    const a = ipToBytes('fe80::1%eth0');
    const b = ipToBytes('fe80::1');
    assert.deepStrictEqual(a, b);
});

test('getClientIp: defaults to socket peer when trustProxy=false', () => {
    const req = {
        headers: { 'x-forwarded-for': '1.2.3.4' },
        socket: { remoteAddress: '10.0.0.1' },
    };
    // Spoofed XFF must be ignored — this is the security-critical case.
    assert.strictEqual(getClientIp(req, false), '10.0.0.1');
});

test('getClientIp: takes leftmost X-Forwarded-For when trustProxy=true', () => {
    const req = {
        headers: { 'x-forwarded-for': '1.2.3.4, 10.0.0.5' },
        socket: { remoteAddress: '127.0.0.1' },
    };
    assert.strictEqual(getClientIp(req, true), '1.2.3.4');
});

test('getClientIp: falls back to socket when XFF missing even with trustProxy=true', () => {
    const req = {
        headers: {},
        socket: { remoteAddress: '10.0.0.1' },
    };
    assert.strictEqual(getClientIp(req, true), '10.0.0.1');
});

test('getClientIp: tolerates missing socket without throwing', () => {
    assert.strictEqual(getClientIp({ headers: {} }, false), '');
});
