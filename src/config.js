'use strict';
require('dotenv').config();

/**
 * Load and validate all OPENINC_MONGO_BACKUP_* environment variables.
 * Provides lazy access so that commands like 'init' can run without a valid environment.
 */

const PREFIX = 'OPENINC_MONGO_BACKUP_';

function get(key, defaultValue) {
  const val = process.env[`${PREFIX}${key}`];
  if (val === undefined || val === '') {
    return defaultValue;
  }
  return val;
}

function require_env(key) {
  const val = process.env[`${PREFIX}${key}`];
  if (!val) {
    throw new Error(`Missing required environment variable: ${PREFIX}${key}. Run 'init' to create a template.`);
  }
  return val;
}

// Backing up or — worse — restoring against MongoDB's internal databases
// (admin/config/local) is almost always operator error. The damage potential
// is asymmetric: a typo here can wipe out the auth database (admin) or the
// shard config (config) of a target cluster. Hard refusal at config load.
const FORBIDDEN_DB_NAMES = new Set(['admin', 'config', 'local']);

function validateDbName(name, fieldName) {
  if (!name) return null;
  if (FORBIDDEN_DB_NAMES.has(name.toLowerCase())) {
    throw new Error(
      `${PREFIX}${fieldName}=${name} is a MongoDB system database — refusing. ` +
      `crashsafe is not designed for backing up or restoring admin/config/local. ` +
      `Set the variable to a real application database, or use mongodump directly with appropriate care.`
    );
  }
  return name;
}

const config = {
  get uri() { return require_env('URI'); },
  // Optional separate destination for `restore`. Lets you replay a backup
  // into a different cluster (e.g. a sandbox) without changing the main URI.
  // Defaults to the same URI used for backups, so existing setups are
  // unchanged. The URI must be reachable from the daemon AND from the
  // mongorestore binary it spawns.
  get restoreUri() { return get('RESTORE_URI', null) || this.uri; },
  get backupDir() { return get('DIR', './backups'); },
  get cron() { return get('CRON', '0 * * * *'); },

  // DB names — null means "skip this DB". System DBs are refused at access
  // time so a typo can't quietly point the daemon at admin/config/local.
  get dbData() { return validateDbName(get('DB_DATA', null) || null, 'DB_DATA'); },
  get dbParse() { return validateDbName(get('DB_PARSE', null) || null, 'DB_PARSE'); },

  // Sensor DB options
  get collectionPrefix() { return get('COLLECTION_PREFIX', 'sensors---'); },
  get sensorConfigCollection() { return get('SENSOR_CONFIG_COLLECTION', 'config'); },
  get uiPort() { return parseInt(get('UI_PORT', '3000'), 10); },
  get updatedAtField() { return get('UPDATED_AT_FIELD', 'updatedAt'); },

  // --- Append-only mode per DB. Default: false → full delete tracking (current behaviour).
  // When true: skips per-collection ID enumeration on incrementals — massively faster
  // for high-volume append-only workloads (e.g. sensor streams) at the cost of NOT
  // detecting deletions. The data DB's config collection is exempt: it always keeps
  // full delete tracking regardless of this setting. ---
  get appendOnlyData() { return get('APPEND_ONLY_DATA', '').toLowerCase() === 'true'; },
  get appendOnlyParse() { return get('APPEND_ONLY_PARSE', '').toLowerCase() === 'true'; },

  // --- Pause between collections during a backup. Default: 300ms.
  // Raise on shared / single-node Mongo deployments where mongodump's read load
  // competes with the live workload — a longer pause gives WiredTiger time to
  // evict and checkpoint between collections, so the live DB's working set is
  // not constantly thrashed. Values in the low seconds (2000-5000) are typical
  // for "polite" backups on a single mongod that also serves an application. ---
  get collectionPauseMs() {
      const n = parseInt(get('COLLECTION_PAUSE_MS', '300'), 10);
      return Number.isFinite(n) && n >= 0 ? n : 300;
  },

  // --- Run mongodump at low CPU + IO priority. Default: false.
  // When true, mongodump is spawned via `nice -n 19 ionice -c 3 mongodump …`,
  // which tells the Linux kernel to give the dump process only idle CPU/IO
  // slices. The live workload always wins under contention — backups take
  // longer, but the host stays responsive. Linux-only; on macOS `nice` exists
  // but `ionice` does not — enabling this on macOS will fail at spawn time. ---
  get niceBackup() { return get('NICE_BACKUP', '').toLowerCase() === 'true'; },

  // --- Scheduled integrity verification. Default: empty → no scheduled verify.
  // When set, the daemon runs `openinc-crashsafe verify` on this cron, separate
  // from the backup cron, so bit-rot / silent corruption gets noticed without
  // an operator manually pressing the button. Failures land in the structured
  // log with `level=error, msg="Scheduled verify found corruption"` so any
  // log-based alerting can pattern-match on it. ---
  get verifyCron() { return get('VERIFY_CRON', '') || null; },
  get verifyDeep() { return get('VERIFY_DEEP', '').toLowerCase() === 'true'; },

  // --- Web Dashboard auth (HTTP Basic). Both must be set to enable; both empty disables auth. ---
  get authUser() { return get('AUTH_USER', null) || null; },
  get authPassword() { return get('AUTH_PASSWORD', null) || null; },

  // --- Web Dashboard IP allowlist. Comma-separated list of IPs/CIDRs (IPv4+IPv6).
  // Empty / unset = no IP gate (any source allowed). Applied before Basic Auth,
  // so unauthorized IPs never reach the auth challenge. ---
  get allowedIps() { return get('ALLOWED_IPS', '') || ''; },

  // Trust X-Forwarded-For for client IP resolution. Only enable when the
  // daemon is reachable exclusively via a known reverse proxy — otherwise
  // an attacker can spoof the header and trivially bypass the allowlist.
  get trustProxy() { return get('TRUST_PROXY', '').toLowerCase() === 'true'; },

  // --- TLS for the dashboard. Both cert and key paths must be set together
  // to enable native HTTPS. When unset, the daemon refuses to start unless
  // ALLOW_INSECURE_HTTP=true acknowledges that TLS is terminated upstream
  // (e.g. by a reverse proxy on the same host). ---
  get tlsCert() { return get('TLS_CERT', null) || null; },
  get tlsKey() { return get('TLS_KEY', null) || null; },

  // Explicit acknowledgement that the operator is fine with the daemon
  // speaking plain HTTP — i.e. they have a TLS-terminating proxy in front
  // and the daemon is not reachable directly from untrusted networks.
  // Without this flag and without TLS_CERT/TLS_KEY, startup is refused.
  get allowInsecureHttp() { return get('ALLOW_INSECURE_HTTP', '').toLowerCase() === 'true'; },
};

module.exports = config;
