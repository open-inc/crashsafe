'use strict';
const { Command } = require('commander');
const fs = require('node:fs');
const { runBackup } = require('./backup');
const { runRestore } = require('./restore');
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
            await runRestore(opts.target, backupId ?? null, opts.full, opts.since ?? null, opts.dropExisting ?? false);
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
