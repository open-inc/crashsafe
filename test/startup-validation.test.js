'use strict';
const { test } = require('node:test');
const assert = require('node:assert');

const { validateAccessGate, resolveTlsMode } = require('../src/startup-validation');

// ---------------------------------------------------------------------------
// validateAccessGate — refuse to start when neither auth nor IP allowlist is set
// ---------------------------------------------------------------------------

test('validateAccessGate: passes when only auth is on', () => {
    assert.doesNotThrow(() => validateAccessGate({ authOn: true, ipAllowlistSize: 0 }));
});

test('validateAccessGate: passes when only IP allowlist is non-empty', () => {
    assert.doesNotThrow(() => validateAccessGate({ authOn: false, ipAllowlistSize: 1 }));
});

test('validateAccessGate: passes when both are configured', () => {
    assert.doesNotThrow(() => validateAccessGate({ authOn: true, ipAllowlistSize: 5 }));
});

test('validateAccessGate: throws when neither is configured', () => {
    assert.throws(
        () => validateAccessGate({ authOn: false, ipAllowlistSize: 0 }),
        err => /no access gate is configured/.test(err.message)
    );
});

test('validateAccessGate: error message mentions both options by name', () => {
    try {
        validateAccessGate({ authOn: false, ipAllowlistSize: 0 });
        assert.fail('expected throw');
    } catch (err) {
        // Operator should be able to copy-paste the variable names from the error.
        assert.match(err.message, /OPENINC_MONGO_BACKUP_AUTH_USER/);
        assert.match(err.message, /OPENINC_MONGO_BACKUP_AUTH_PASSWORD/);
        assert.match(err.message, /OPENINC_MONGO_BACKUP_ALLOWED_IPS/);
    }
});

// ---------------------------------------------------------------------------
// resolveTlsMode — refuse plain HTTP unless explicitly opted in
// ---------------------------------------------------------------------------

test('resolveTlsMode: returns https when both cert and key are set and readable', () => {
    const fakeReader = (p) => p === '/cert' ? Buffer.from('CERT') : Buffer.from('KEY');
    const out = resolveTlsMode(
        { tlsCert: '/cert', tlsKey: '/key', allowInsecureHttp: false },
        { readFile: fakeReader }
    );
    assert.strictEqual(out.mode, 'https');
    assert.deepStrictEqual(out.cert, Buffer.from('CERT'));
    assert.deepStrictEqual(out.key, Buffer.from('KEY'));
});

test('resolveTlsMode: throws when only TLS_CERT is set (half-config)', () => {
    assert.throws(
        () => resolveTlsMode({ tlsCert: '/cert', tlsKey: null, allowInsecureHttp: true }),
        /TLS_CERT and OPENINC_MONGO_BACKUP_TLS_KEY must both be set/
    );
});

test('resolveTlsMode: throws when only TLS_KEY is set (half-config)', () => {
    assert.throws(
        () => resolveTlsMode({ tlsCert: null, tlsKey: '/key', allowInsecureHttp: true }),
        /TLS_CERT and OPENINC_MONGO_BACKUP_TLS_KEY must both be set/
    );
});

test('resolveTlsMode: throws with helpful message when files cannot be read', () => {
    const broken = () => { throw new Error('ENOENT'); };
    assert.throws(
        () => resolveTlsMode(
            { tlsCert: '/missing', tlsKey: '/also-missing', allowInsecureHttp: false },
            { readFile: broken }
        ),
        /Cannot read TLS files/
    );
});

test('resolveTlsMode: returns http when ALLOW_INSECURE_HTTP=true and no TLS', () => {
    const out = resolveTlsMode({ tlsCert: null, tlsKey: null, allowInsecureHttp: true });
    assert.deepStrictEqual(out, { mode: 'http' });
});

test('resolveTlsMode: refuses plain HTTP without explicit opt-in', () => {
    assert.throws(
        () => resolveTlsMode({ tlsCert: null, tlsKey: null, allowInsecureHttp: false }),
        /plain HTTP is not allowed by default/
    );
});

test('resolveTlsMode: error names both TLS option and the opt-out flag', () => {
    try {
        resolveTlsMode({ tlsCert: null, tlsKey: null, allowInsecureHttp: false });
        assert.fail('expected throw');
    } catch (err) {
        assert.match(err.message, /OPENINC_MONGO_BACKUP_TLS_CERT/);
        assert.match(err.message, /OPENINC_MONGO_BACKUP_TLS_KEY/);
        assert.match(err.message, /OPENINC_MONGO_BACKUP_ALLOW_INSECURE_HTTP/);
    }
});
