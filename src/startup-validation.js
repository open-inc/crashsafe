'use strict';
const fs = require('node:fs');

// ---------------------------------------------------------------------------
// Pure startup-time validators for the dashboard server.
// Kept dependency-free (no config / dotenv) so they unit-test cleanly.
// ---------------------------------------------------------------------------

/**
 * Refuse to start unless at least one access gate (Basic Auth OR IP allowlist)
 * is configured. The historical default was "no gate" — anyone reaching the
 * port could trigger destructive endpoints. This eliminates that footgun.
 */
function validateAccessGate({ authOn, ipAllowlistSize }) {
    if (authOn || ipAllowlistSize > 0) return;

    const lines = [
        'CrashSafe refuses to start: no access gate is configured.',
        'At least ONE of the following MUST be set, otherwise the dashboard would be open to anyone who can reach the port:',
        '',
        '  Option A — HTTP Basic Auth (recommended for human operators)',
        '    OPENINC_MONGO_BACKUP_AUTH_USER=<username>',
        '    OPENINC_MONGO_BACKUP_AUTH_PASSWORD=<password>',
        '',
        '  Option B — IP allowlist (recommended for machine-only access)',
        '    OPENINC_MONGO_BACKUP_ALLOWED_IPS=<csv of IPs/CIDRs, e.g. 10.0.0.0/8,192.168.1.5>',
        '',
        'Both can also be combined (defense in depth).',
    ];
    throw new Error(lines.join('\n'));
}

/**
 * Resolve the TLS configuration for the listener. Returns either:
 *  - { mode: 'https', key, cert } when TLS_CERT + TLS_KEY are set, OR
 *  - { mode: 'http' } when ALLOW_INSECURE_HTTP=true (operator acknowledges that
 *    TLS is terminated upstream by a reverse proxy on the same trust boundary).
 *
 * Throws otherwise — the daemon refuses to expose Basic-Auth-protected
 * endpoints over plain HTTP unless the operator explicitly opts in.
 *
 * The `readFile` parameter exists so tests can inject without touching disk.
 */
function resolveTlsMode({ tlsCert, tlsKey, allowInsecureHttp }, { readFile = fs.readFileSync } = {}) {
    const hasCert = Boolean(tlsCert);
    const hasKey = Boolean(tlsKey);

    if (hasCert !== hasKey) {
        throw new Error(
            'OPENINC_MONGO_BACKUP_TLS_CERT and OPENINC_MONGO_BACKUP_TLS_KEY must both be set or both unset.'
        );
    }

    if (hasCert && hasKey) {
        let cert, key;
        try {
            cert = readFile(tlsCert);
            key = readFile(tlsKey);
        } catch (err) {
            throw new Error(
                `Cannot read TLS files (TLS_CERT=${tlsCert}, TLS_KEY=${tlsKey}): ${err.message}`
            );
        }
        return { mode: 'https', cert, key };
    }

    if (allowInsecureHttp) {
        return { mode: 'http' };
    }

    const lines = [
        'CrashSafe refuses to start: plain HTTP is not allowed by default.',
        'Basic Auth credentials over plain HTTP are base64-encoded, not encrypted, so a passive observer can read them.',
        'Choose ONE of:',
        '',
        '  Option A — Native HTTPS (terminate TLS in the daemon)',
        '    OPENINC_MONGO_BACKUP_TLS_CERT=/path/to/fullchain.pem',
        '    OPENINC_MONGO_BACKUP_TLS_KEY=/path/to/privkey.pem',
        '',
        '  Option B — Plain HTTP behind a TLS-terminating reverse proxy',
        '    OPENINC_MONGO_BACKUP_ALLOW_INSECURE_HTTP=true',
        '    (Only enable when the daemon is reachable EXCLUSIVELY via your reverse proxy.',
        '     If the daemon is reachable directly from any untrusted network, attackers can sniff credentials.)',
    ];
    throw new Error(lines.join('\n'));
}

module.exports = { validateAccessGate, resolveTlsMode };
