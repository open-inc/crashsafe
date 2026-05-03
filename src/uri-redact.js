'use strict';

/**
 * Replace the password portion of every MongoDB connection string in `s` with
 * `***`. Idempotent and safe to call on strings that contain zero or many URIs.
 *
 * The username is left intact because (a) it isn't a secret and (b) it's
 * useful diagnostic info ("connection failed for user X"). The password is
 * the only field we mask.
 *
 * Handles both the common `mongodb://` form and the SRV form `mongodb+srv://`.
 * Stops at the first unencoded `@` because that's the URI's userinfo→host
 * separator; passwords with literal `@` must be percent-encoded by spec, so
 * `%40` doesn't trigger the boundary.
 */
function redactUri(s) {
    if (typeof s !== 'string') return s;
    return s.replace(
        /(mongodb(?:\+srv)?:\/\/[^:@/\s]+):[^@/\s]+@/g,
        '$1:***@',
    );
}

/**
 * Sanitize an Error / spawn-result object before logging. Returns a plain
 * object with redacted versions of every commonly-leaky field, plus the
 * stack and code/codeName so the log is still actionable.
 *
 * Background: `child_process.execFile` rejects with an Error whose `cmd`
 * property is the full spawned command — for `mongodump --uri=mongodb://u:p@h`
 * that string contains the password. Logging the error directly via pino
 * dumps every enumerable field, which leaks the password into structured
 * logs. This helper keeps the diagnostic value (message, stack, code) but
 * passes the leaky strings through {@link redactUri}.
 */
function redactErr(err) {
    if (!err || typeof err !== 'object') return err;
    const out = {};
    if (err.name)     out.name = err.name;
    if (err.message)  out.message = redactUri(err.message);
    if (err.code)     out.code = err.code;
    if (err.codeName) out.codeName = err.codeName;
    if (err.signal)   out.signal = err.signal;
    if (err.cmd)      out.cmd = redactUri(err.cmd);
    if (err.stdout)   out.stdout = redactUri(String(err.stdout));
    if (err.stderr)   out.stderr = redactUri(String(err.stderr));
    if (err.stack)    out.stack = redactUri(err.stack);
    return out;
}

module.exports = { redactUri, redactErr };
