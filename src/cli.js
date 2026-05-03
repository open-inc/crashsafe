'use strict';
const { Command } = require('commander');
const fs = require('node:fs');
const { runBackup } = require('./backup');
const { runRestore } = require('./restore');
const { runVerify } = require('./verify');
const { readManifest } = require('./manifest');
const config = require('./config');
const logger = require('./logger');
const path = require('node:path');

const program = new Command();

program
    .name('openinc-backup')
    .description('OpenInc MongoDB incremental backup and restore tool')
    .version('1.0.0');

// ---------------------------------------------------------------------------
// backup command
// ---------------------------------------------------------------------------
program
    .command('backup')
    .description('Run a backup now (incremental by default)')
    .option('--full', 'Force a full backup instead of incremental', false)
    .option('--dry-run', 'Plan only — list what would be dumped, no files written, no manifest mutation', false)
    .action(async (opts) => {
        try {
            const result = await runBackup({ full: opts.full, trigger: 'cli', dryRun: !!opts.dryRun });
            if (result?.skipped) {
                logger.warn({ reason: result.reason, holder: result.holder }, 'Backup skipped: another run is already in progress');
                process.exit(1);
            }
            if (result?.dryRun) {
                console.log('\n=== DRY RUN — no files written, no manifest mutation ===\n');
                for (const r of result.results) {
                    console.log(`[${r.dbType}] type=${r.type} id=${r.id}`);
                    console.log(`  ${r.collections.length} collection(s) inspected, ${r.trackingOps} tracking op(s) would be written`);
                    for (const c of r.collections) {
                        if (!c.wouldDump && c.upserts === 0 && c.deletes === 0) continue;
                        console.log(`  - ${c.name}: would-dump=${c.wouldDump}  upserts=${c.upserts}  deletes=${c.deletes}  mode=${c.mode}`);
                    }
                    console.log('');
                }
                process.exit(0);
            }
            for (const r of result) {
                logger.info(r, 'Backup finished');
            }
            process.exit(0);
        } catch (err) {
            logger.error({ err }, 'Backup failed');
            process.exit(1);
        }
    });

// ---------------------------------------------------------------------------
// restore command
// ---------------------------------------------------------------------------
program
    .command('restore')
    .description('Restore from a backup')
    .argument('[backupId]', 'Backup ID (ISO timestamp). Defaults to the latest backup.')
    .option('--full', 'Replay entire backup chain (point-in-time restore)', false)
    .option('--dropExisting', 'Drop all existing collections before restoring (requires --full)', false)
    .option(
        '--since <backupId>',
        'Replay all backups from this ID (inclusive) to [backupId] or latest, without dropping data'
    )
    .option(
        '--target <db>',
        'Which DB to restore: data, parse, or all (default: all)',
        'all'
    )
    .option(
        '--verify-checksums',
        'Re-hash every file in the chain against stored SHA-256 BEFORE the restore runs. Slow on large chains, but catches silent on-disk corruption before the live DB is touched. Recommended whenever --dropExisting is used.',
        false
    )
    .option(
        '--yes-i-am-sure-this-wipes <name>',
        'Required when --dropExisting is set. Pass the exact name of the DB you intend to wipe (the value of OPENINC_MONGO_BACKUP_DB_DATA / DB_PARSE), or the literal "all" when --target=all. Refuses the restore if the value does not match.',
        null
    )
    .option(
        '--dry-run',
        'Plan only — list the chain that would be replayed, run pre-flight, but do not drop/replay anything',
        false
    )
    .option(
        '--mode <mode>',
        'Restore strategy: "direct" (default) drops the live DB then replays — fast, no extra disk, but a mid-replay failure leaves a half-restored DB. "sidecar" replays into a shadow DB and only swaps on success — 2× disk and 2× time, but the live DB is byte-for-byte unchanged on any replay failure. Sidecar requires --full and ignores --dropExisting (the swap IS the destruction).',
        'direct'
    )
    .action(async (backupId, opts) => {
        try {
            if (!['direct', 'sidecar'].includes(opts.mode)) {
                console.error(`Invalid --mode "${opts.mode}". Expected "direct" or "sidecar".`);
                process.exit(1);
            }
            // Sidecar always replaces the live DB on swap, so it's destructive
            // even though --dropExisting isn't passed. Treat the same gate
            // (typed-name confirmation) as for direct + --dropExisting.
            const isDestructive = opts.dropExisting || opts.mode === 'sidecar';
            // Guard against fat-finger destructive runs at the CLI layer. The
            // API uses single-use tokens; the CLI uses a typed-name match
            // because no HTTP round-trip is involved. In --dry-run we skip
            // this gate because nothing is actually destructive.
            if (isDestructive && !opts.dryRun) {
                const target = opts.target;
                const expected = [];
                if ((target === 'data'  || target === 'all') && config.dbData)  expected.push(config.dbData);
                if ((target === 'parse' || target === 'all') && config.dbParse) expected.push(config.dbParse);
                if (target === 'all') expected.push('all');

                const provided = opts.yesIAmSureThisWipes;
                if (!provided) {
                    console.error(
                        `\nDestructive restore refused.\n` +
                        `  --dropExisting requires --yes-i-am-sure-this-wipes <name>.\n` +
                        `  Acceptable values for --target=${target}: ${expected.join(', ')}\n`
                    );
                    process.exit(1);
                }
                if (!expected.includes(provided)) {
                    console.error(
                        `\nDestructive restore refused.\n` +
                        `  --yes-i-am-sure-this-wipes value "${provided}" does not match the configured target.\n` +
                        `  Acceptable values for --target=${target}: ${expected.join(', ')}\n`
                    );
                    process.exit(1);
                }
            }

            const result = await runRestore(
                opts.target,
                backupId ?? null,
                opts.full,
                opts.since ?? null,
                opts.dropExisting ?? false,
                'cli',
                {
                    verifyChecksums: !!opts.verifyChecksums,
                    dryRun: !!opts.dryRun,
                    mode: opts.mode,
                },
            );
            if (result?.skipped) {
                logger.warn({ reason: result.reason, holder: result.holder }, 'Restore skipped: another operation is already in progress');
                process.exit(1);
            }
            if (result?.dryRun) {
                console.log('\n=== DRY RUN — no drops, no mongorestore, no live-DB writes ===\n');
                console.log(`Target: ${result.target}  backupId=${result.backupId ?? '(latest)'}  sinceId=${result.sinceId ?? '-'}`);
                console.log(`Mode: full=${result.full} dropExisting=${result.dropExisting} verifyChecksums=${result.verifyChecksums}`);
                console.log(`Restore destination URI: ${result.restoreUri}\n`);
                for (const db of result.dbs) {
                    if (db.error) {
                        console.log(`[${db.dbType}/${db.dbName}] PRE-FLIGHT FAILED — ${db.error}\n`);
                        continue;
                    }
                    console.log(`[${db.dbType}/${db.dbName}] chain length ${db.chainLength}:`);
                    for (const e of db.chain) {
                        const flags = [
                            e.type,
                            e.hasFile ? 'dump' : '∅',
                            e.hasTracking ? 'tracking' : '∅',
                        ].join(' · ');
                        console.log(`  - ${e.id}  (${flags})`);
                    }
                    console.log('');
                }
                process.exit(0);
            }
            process.exit(0);
        } catch (err) {
            logger.error({ err }, 'Restore failed');
            process.exit(1);
        }
    });

// ---------------------------------------------------------------------------
// list command
// ---------------------------------------------------------------------------
program
    .command('list')
    .description('List available backups')
    .option('--target <db>', 'Which DB to list: data, parse, or all (default: all)', 'all')
    .action((opts) => {
        const targets = [];
        if ((opts.target === 'data' || opts.target === 'all') && config.dbData) {
            targets.push({ dbType: 'data', dbName: config.dbData });
        }
        if ((opts.target === 'parse' || opts.target === 'all') && config.dbParse) {
            targets.push({ dbType: 'parse', dbName: config.dbParse });
        }

        for (const { dbType, dbName } of targets) {
            const dir = path.resolve(config.backupDir, dbType);
            const { backups } = readManifest(dir);
            console.log(`\n=== ${dbType.toUpperCase()} (${dbName}) — ${backups.length} backup(s) ===`);
            if (!backups.length) {
                console.log('  (no backups yet)');
                continue;
            }
            for (const b of backups) {
                const cols = b.collections.length ? b.collections.join(', ') : '(empty)';
                console.log(`  [${b.id}]  type=${b.type}  collections=${cols}`);
            }
        }
        console.log('');
    });

// ---------------------------------------------------------------------------
// verify command
// ---------------------------------------------------------------------------
// Exit codes are part of the contract for cron/CI integration:
//   0 = all good
//   1 = something is corrupt, missing, or the manifest itself is broken
//   2 = nothing is corrupt, but at least one entry has no checksum baseline
//       (i.e. it predates this feature). Lets cron pipes treat legacy entries
//       as a soft warning while still failing on real damage.
program
    .command('verify')
    .description('Verify backup integrity by re-hashing files against stored checksums')
    .option('--target <db>', 'Which DB to verify: data, parse, or all (default: all)', 'all')
    .option('--id <backupId>', 'Verify only this backup ID (default: every entry)')
    .option('--deep', 'Also run gunzip -t over each .gz dump (catches valid-hash-but-broken-gzip)', false)
    .option('--json', 'Emit machine-readable JSON instead of human-readable text', false)
    .action(async (opts) => {
        try {
            const result = await runVerify({
                target: opts.target,
                backupId: opts.id ?? null,
                deep: !!opts.deep,
                trigger: 'cli',
            });

            if (result?.skipped) {
                logger.warn({ reason: result.reason, holder: result.holder }, 'Verify skipped: another operation is in progress');
                process.exit(1);
            }

            if (opts.json) {
                console.log(JSON.stringify(result, null, 2));
            } else {
                const { summary, details } = result;
                console.log(`\nVerify: ${summary.ok} ok / ${summary.corrupt} corrupt / ${summary.noBaseline} no-baseline / ${summary.manifestErrors} manifest-error`);
                for (const d of details) {
                    if (d.status === 'ok') continue;
                    if (d.status === 'manifest-error') {
                        console.log(`  [${d.dbType}] MANIFEST: ${d.issues[0]?.reason ?? 'unparseable'}`);
                        continue;
                    }
                    if (d.status === 'no-baseline') {
                        console.log(`  [${d.dbType}/${d.entryId}] no-baseline (entry predates checksum tracking)`);
                        continue;
                    }
                    console.log(`  [${d.dbType}/${d.entryId}] CORRUPT — ${d.issues.length} issue(s):`);
                    for (const i of d.issues.slice(0, 10)) {
                        const where = `${i.kind}/${i.path}`;
                        console.log(`      ${i.status.padEnd(12)} ${where}${i.reason ? ' — ' + i.reason : ''}`);
                    }
                    if (d.issues.length > 10) console.log(`      ... ${d.issues.length - 10} more`);
                }
                console.log('');
            }

            // Hard failure if anything is genuinely broken; soft (exit 2) for
            // legacy entries only.
            if (result.summary.corrupt > 0 || result.summary.manifestErrors > 0) process.exit(1);
            if (result.summary.noBaseline > 0) process.exit(2);
            process.exit(0);
        } catch (err) {
            logger.error({ err }, 'Verify failed');
            process.exit(1);
        }
    });

// ---------------------------------------------------------------------------
// init command
// ---------------------------------------------------------------------------
const ENV_TEMPLATE = `# MongoDB connection string (server root — no database name)
OPENINC_MONGO_BACKUP_URI=mongodb://localhost:27017

# Directory where backups are stored
OPENINC_MONGO_BACKUP_DIR=./backups

# Cron schedule for automatic backups (default: every hour)
OPENINC_MONGO_BACKUP_CRON=0 * * * *

# Name of the sensor/data database to back up (omit to skip)
OPENINC_MONGO_BACKUP_DB_DATA=

# Name of the Parse server database to back up (omit to skip)
OPENINC_MONGO_BACKUP_DB_PARSE=

# Prefix used to identify sensor collections (default: sensors---)
OPENINC_MONGO_BACKUP_COLLECTION_PREFIX=sensors---

# Config collection always fully backed up on every run (default: config)
OPENINC_MONGO_BACKUP_SENSOR_CONFIG_COLLECTION=config

# --- Change Detection Options ---
# Field name used for change detection (default: updatedAt)
OPENINC_MONGO_BACKUP_UPDATED_AT_FIELD=updatedAt

# --- Append-Only Mode (per database) ---
# Skips ID enumeration + delete detection for that DB on incrementals — much faster
# for hot append-only streams (sensors), at the cost of not capturing deletions.
# The data DB's config collection is exempt and always keeps full tracking.
# Default: false (full delete tracking).
# OPENINC_MONGO_BACKUP_APPEND_ONLY_DATA=false
# OPENINC_MONGO_BACKUP_APPEND_ONLY_PARSE=false

# --- Scheduled integrity verification ---
# Optional second cron schedule that runs the verify command (re-hashing every
# backup file against the SHA-256 stored at write time). Without this, bit-rot
# and silent on-disk corruption stay undetected until restore time.
#
# Recommended: nightly at off-peak. Failures log at error level with the
# message "Scheduled verify found corruption" — wire that into your alerting.
# Empty / unset = no scheduled verify (only on-demand via UI / CLI).
# OPENINC_MONGO_BACKUP_VERIFY_CRON=0 4 * * *

# Set to true to also run "gunzip -t" over every dump file during the
# scheduled verify (catches valid-hash-but-broken-gzip cases that the
# regular SHA-only verify cannot see). Slower; default off.
# OPENINC_MONGO_BACKUP_VERIFY_DEEP=false

# --- UI Options ---
# Port for the Web Dashboard (default: 3000)
OPENINC_MONGO_BACKUP_UI_PORT=3000

# Optional HTTP Basic Auth for the dashboard.
# Set BOTH to enable auth; leave BOTH unset to disable. Setting only one is rejected at startup.
# OPENINC_MONGO_BACKUP_AUTH_USER=admin
# OPENINC_MONGO_BACKUP_AUTH_PASSWORD=changeme

`;

program
    .command('init')
    .description('Create a .env configuration file in the current directory')
    .option('--force', 'Overwrite an existing .env file', false)
    .action((opts) => {
        const dest = path.join(process.cwd(), '.env');
        if (fs.existsSync(dest) && !opts.force) {
            console.error(`\n.env already exists. Use --force to overwrite.\n`);
            process.exit(1);
        }
        fs.writeFileSync(dest, ENV_TEMPLATE, 'utf-8');
        console.log(`\nCreated ${dest}\nEdit the file and fill in your database names, then run:\n  openinc-backup backup\n`);
    });

module.exports = program;
