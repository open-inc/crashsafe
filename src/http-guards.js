'use strict';

// ---------------------------------------------------------------------------
// HTTP request guards: CSRF / Origin gate, body-size limit, content-type.
// ---------------------------------------------------------------------------
//
// Pure helpers, no side effects on app state — kept here so they can be unit
// tested without spinning up a server.

const DEFAULT_MAX_BODY_BYTES = 256 * 1024; // 256 KiB

/**
 * Compares the request's Origin (or Referer) host against its Host header.
 * Returns true only if the request is unambiguously same-origin. Used as a
 * CSRF gate for state-changing requests:
 *
 *  - Modern browsers send `Origin` on every cross-origin POST.
 *  - Same-origin POSTs may omit Origin; in that case we fall back to Referer.
 *  - If neither header is present, we err on the side of rejecting. That
 *    blocks form-based CSRF (forms always send Origin on POST in modern
 *    browsers), at the cost of breaking unconfigured curl scripts — which
 *    can be worked around by sending a matching Origin header.
 *
 * Implementation note: we compare on the URL `host` (incl. port), not on
 * scheme. The reverse-proxy case rewrites the scheme, but Host stays the
 * same as what the client used — so a host-only compare is the robust check.
 */
function isSameOriginRequest(req) {
    const host = req.headers && req.headers.host;
    if (!host) return false;

    const origin = req.headers.origin;
    const referer = req.headers.referer;

    if (typeof origin === 'string' && origin.length > 0) {
        // Some clients legitimately send `Origin: null` (sandboxed iframes,
        // file:// pages, redirects between schemes). Treat as cross-origin.
        if (origin === 'null') return false;
        try {
            return new URL(origin).host === host;
        } catch {
            return false;
        }
    }

    if (typeof referer === 'string' && referer.length > 0) {
        try {
            return new URL(referer).host === host;
        } catch {
            return false;
        }
    }

    return false;
}

/** Lower-cased Content-Type without parameters (`application/json; charset=…` → `application/json`). */
function contentTypeBase(req) {
    const raw = (req.headers && req.headers['content-type']) || '';
    return raw.split(';')[0].trim().toLowerCase();
}

/**
 * Read the request body up to `maxBytes`. If the limit is exceeded, the
 * connection is destroyed and the promise rejects with `code: 'EBODYTOOLARGE'`.
 * The connection is killed (rather than just draining) so a malicious client
 * can't keep streaming past the cap.
 */
function readBodyWithLimit(req, maxBytes = DEFAULT_MAX_BODY_BYTES) {
    return new Promise((resolve, reject) => {
        const chunks = [];
        let total = 0;
        let settled = false;

        const finish = (fn, val) => {
            if (settled) return;
            settled = true;
            fn(val);
        };

        req.on('data', chunk => {
            if (settled) return;
            // Real HTTP requests emit Buffers; encoded streams emit strings.
            // Normalise so byte-counting and concat are correct in both cases.
            const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
            total += buf.length;
            if (total > maxBytes) {
                const err = new Error(`request body exceeds ${maxBytes} bytes`);
                err.code = 'EBODYTOOLARGE';
                // Sever the connection so the client can't keep streaming.
                try { req.destroy(); } catch { /* ignore */ }
                finish(reject, err);
                return;
            }
            chunks.push(buf);
        });
        req.on('end', () => finish(resolve, Buffer.concat(chunks).toString('utf-8')));
        req.on('error', err => finish(reject, err));
    });
}

module.exports = {
    isSameOriginRequest,
    contentTypeBase,
    readBodyWithLimit,
    DEFAULT_MAX_BODY_BYTES,
};
