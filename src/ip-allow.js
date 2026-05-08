'use strict';
const net = require('node:net');

// ---------------------------------------------------------------------------
// IP allowlist for the dashboard / API.
// ---------------------------------------------------------------------------
//
// Sits in front of HTTP Basic Auth as a second, network-layer gate. Configured
// via OPENINC_MONGO_BACKUP_ALLOWED_IPS as a comma-separated list of single IPs
// or CIDR ranges (IPv4 + IPv6 both supported). An empty list disables the
// gate entirely so existing setups stay unchanged.
//
// X-Forwarded-For is *only* trusted when OPENINC_MONGO_BACKUP_TRUST_PROXY=true.
// Without that flag a remote attacker could spoof the header and bypass the
// allowlist trivially — so the default is the socket peer address.

function ipToBytes(ip) {
    if (typeof ip !== 'string' || ip.length === 0) return null;

    // IPv6 zone id (e.g. fe80::1%eth0) — strip before parsing.
    const pct = ip.indexOf('%');
    if (pct !== -1) ip = ip.slice(0, pct);

    // IPv4-mapped IPv6 (Node returns these for v4 connections on a v6 socket).
    const v4Mapped = /^::ffff:(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$/i.exec(ip);
    if (v4Mapped) ip = v4Mapped[1];

    if (net.isIPv4(ip)) {
        const parts = ip.split('.').map(Number);
        if (parts.some(p => !Number.isFinite(p) || p < 0 || p > 255)) return null;
        return Buffer.from(parts);
    }

    if (net.isIPv6(ip)) return ipv6ToBytes(ip);

    return null;
}

function ipv6ToBytes(ip) {
    let parts;
    const dc = ip.indexOf('::');
    if (dc !== -1) {
        const left = ip.slice(0, dc).split(':').filter(s => s !== '');
        const right = ip.slice(dc + 2).split(':').filter(s => s !== '');
        const missing = 8 - left.length - right.length;
        if (missing < 0) return null;
        parts = [...left, ...Array(missing).fill('0'), ...right];
    } else {
        parts = ip.split(':');
    }
    if (parts.length !== 8) return null;

    const bytes = Buffer.alloc(16);
    for (let i = 0; i < 8; i++) {
        if (!/^[0-9a-f]{1,4}$/i.test(parts[i])) return null;
        const v = parseInt(parts[i], 16);
        bytes[i * 2] = (v >> 8) & 0xff;
        bytes[i * 2 + 1] = v & 0xff;
    }
    return bytes;
}

function parseEntry(raw) {
    const entry = raw.trim();
    if (!entry) return null;

    const slash = entry.indexOf('/');
    let ipPart;
    let prefix;
    if (slash === -1) {
        ipPart = entry;
    } else {
        ipPart = entry.slice(0, slash);
        prefix = Number(entry.slice(slash + 1));
        if (!Number.isInteger(prefix) || prefix < 0) return null;
    }

    const bytes = ipToBytes(ipPart);
    if (!bytes) return null;

    const maxPrefix = bytes.length * 8;
    if (prefix === undefined) prefix = maxPrefix;
    if (prefix > maxPrefix) return null;

    return { bytes, prefix, source: entry };
}

/**
 * Parse a comma-separated allowlist string into normalized entries.
 * Throws on malformed input so misconfiguration fails loud at startup
 * instead of silently locking everyone out.
 */
function parseAllowList(str) {
    if (!str || typeof str !== 'string') return [];
    const out = [];
    for (const raw of str.split(',')) {
        if (!raw.trim()) continue;
        const e = parseEntry(raw);
        if (!e) {
            throw new Error(`Invalid IP/CIDR in allowlist: "${raw.trim()}"`);
        }
        out.push(e);
    }
    return out;
}

function bytesMatch(a, b, prefixBits) {
    if (a.length !== b.length) return false;
    const fullBytes = prefixBits >> 3;
    const remainingBits = prefixBits & 7;

    for (let i = 0; i < fullBytes; i++) {
        if (a[i] !== b[i]) return false;
    }
    if (remainingBits === 0) return true;
    const mask = (0xff << (8 - remainingBits)) & 0xff;
    return (a[fullBytes] & mask) === (b[fullBytes] & mask);
}

/** True if `ip` falls inside any entry of `allowList`. Empty list = allow all. */
function isAllowed(ip, allowList) {
    if (!allowList || allowList.length === 0) return true;
    const ipBytes = ipToBytes(ip);
    if (!ipBytes) return false;
    for (const entry of allowList) {
        if (bytesMatch(ipBytes, entry.bytes, entry.prefix)) return true;
    }
    return false;
}

/**
 * Resolve the client's remote IP. Only consults X-Forwarded-For when
 * `trustProxy` is true — without that, the header is attacker-controlled
 * and using it would defeat the allowlist entirely.
 */
function getClientIp(req, trustProxy) {
    if (trustProxy) {
        const xff = req.headers && req.headers['x-forwarded-for'];
        if (typeof xff === 'string' && xff.length > 0) {
            // Leftmost = original client (as set by the first proxy in the chain).
            const first = xff.split(',')[0].trim();
            if (first) return first;
        }
    }
    return (req.socket && req.socket.remoteAddress) || '';
}

module.exports = { parseAllowList, isAllowed, getClientIp, ipToBytes };
