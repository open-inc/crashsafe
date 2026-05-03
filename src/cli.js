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
    .action(async (opts) => {
        try {
            const result = await runBackup({ full: opts.full, trigger: 'cli' });
            if (result?.skipped) {
                logger.warn({ reason: result.reason, holder: result.holder }, 'Backup skipped: another run is already in progress');
                process.exit(1);
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
    .action(async (backupId, opts) => {
        try {
            const result = await runRestore(opts.target, backupId ?? null, opts.full, opts.since ?? null, opts.dropExisting ?? false, 'cli');
            if (result?.skipped) {
                logger.warn({ reason: result.reason, holder: result.holder }, 'Restore skipped: another operation is already in progress');
                process.exit(1);
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
