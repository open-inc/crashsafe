# MongoDB CrashSafe

A robust Node.js daemon + CLI for **incremental MongoDB backups** using native `mongodump`. It perfectly preserves BSON types (ObjectIDs, Dates, etc.) and features a premium Web Dashboard for monitoring and manual triggers.

## Features

- **BSON Native**: Uses `mongodump` and `mongorestore` for maximum reliability and type safety.
- **Incremental Logic**: Efficiently backs up only changed documents using `updatedAt` timestamps.
- **Deletion Tracking**: Automatically tracks and replays document deletions using lightweight ID snapshots.
- **Web Dashboard**: Premium status page at `http://localhost:3000` to monitor health and trigger manual runs.
- **Scheduled**: Runs as a background daemon with a configurable cron schedule.
- **Selective Restore**: Pick any point in the backup chain to "roll forward" or perform a full wipe-and-restore.

## Requirements

- **Node.js** >= 18.0.0
- **MongoDB Database Tools**: `mongodump` and `mongorestore` must be installed and available in your `PATH`.

---

## Setup

### Global Install

```bash
npm install -g .
openinc-crashsafe init
# Edit the generated .env file
openinc-crashsafe
```

### Docker

The included `Dockerfile` bundles Node.js with the necessary MongoDB tools.

```bash
docker build -t openinc-crashsafe .
docker run -d --env-file .env -p 3000:3000 openinc-crashsafe
```

---

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `OPENINC_MONGO_BACKUP_URI` | ✅ | — | MongoDB connection string |
| `OPENINC_MONGO_BACKUP_DB_DATA` | ⚠️ | — | Name of the data database. |
| `OPENINC_MONGO_BACKUP_DB_PARSE` | ⚠️ | — | Name of the Parse server database. |
| `OPENINC_MONGO_BACKUP_DIR` | ❌ | `./backups` | Root directory for backups |
| `OPENINC_MONGO_BACKUP_CRON` | ❌ | `0 * * * *` | Cron expression |
| `OPENINC_MONGO_BACKUP_UI_PORT` | ❌ | `3000` | Port for the Web Dashboard |
| `OPENINC_MONGO_BACKUP_COLLECTION_PREFIX` | ❌ | `sensors---` | Filter for data collections |
| `OPENINC_MONGO_BACKUP_UPDATED_AT_FIELD` | ❌ | `updatedAt` | Field name for change detection |

---

## Usage

### Web Dashboard
Start the daemon and visit `http://localhost:3000`. You can monitor recent runs and trigger manual backups or restores per database.

### CLI Commands

```bash
# Start the daemon
openinc-crashsafe

# Run a manual incremental backup
openinc-crashsafe backup

# Run a manual full backup
openinc-crashsafe backup --full

# List backup history
openinc-crashsafe list

# Restore the latest state (incremental)
openinc-crashsafe restore

# Wipe database and restore everything to a specific ID
openinc-crashsafe restore <ID> --full
```

---

## Backup Format

Backups are stored in `backups/<dbType>/<ID>/`.

- **`<ID>/`**: A directory containing `mongodump` BSON archives.
- **`<ID>.tracking.json`**: Records deleted and updated IDs for precise incremental replaying.
- **`ids/<ID>.json`**: Lightweight snapshot of all IDs used for change detection.
- **`manifest.json`**: The index of all backup runs.

### How it works
1. **Change Detection**: We fetch all IDs from the DB and compare them against the last run's snapshot to find **deleted** documents.
2. **Incremental Dump**: We run `mongodump --query` to fetch only documents modified since the last run.
3. **Restore**: We first delete any modified/deleted IDs via the JavaScript driver, then run `mongorestore` to upsert the new data safely.

---

## License
MIT
