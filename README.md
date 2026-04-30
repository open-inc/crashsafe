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
- **`ids/<ID>/<collection>.jsonl`**: Per-collection ID snapshot (one EJSON-encoded `_id` per line) used for delete detection on the next run.
- **`manifest.json`**: The index of all backup runs.

### How it works
1. **Change Detection**: For each collection we stream all `_id`s from the DB and compare against the previous run's snapshot file to find **deleted** documents.
2. **Incremental Dump**: We run `mongodump --query` to fetch only documents modified since the last run.
3. **Restore**: We first delete any modified/deleted IDs via the JavaScript driver, then run `mongorestore` to upsert the new data safely.

---

## Concurrency

Only one backup runs at a time, even with a tight cron schedule, multiple manual API clicks, or a CLI invocation overlapping with the daemon.

- **In-process mutex**: cron + API triggers within the daemon are serialized by an in-memory flag. If a backup is already running, the new trigger is **skipped** (logged as `skipped`) — it does not queue.
- **Cross-process lockfile**: `<backupDir>/.backup.lock` (JSON: `{ pid, startedAt, trigger }`) is created atomically with `O_EXCL` and removed on completion. A second process (e.g. a CLI run while the daemon runs) sees the file and exits skipped.
- **Stale lock recovery**: on startup, an existing lockfile is reclaimed if (a) the recorded PID is no longer alive, or (b) the lock is older than 24h. This handles SIGKILL / OOM / power-loss cases without manual cleanup.
- **Clean shutdown**: SIGINT/SIGTERM and normal `process.exit` paths remove the lockfile via a `process.on('exit')` hook.

If you ever need to force-clear the lock manually: `rm <backupDir>/.backup.lock`.

---

## Resource Usage

Initial full backups can touch hundreds of collections with millions of documents each. To keep both the Node process and `mongod` stable on memory-constrained servers, the backup loop is designed to hold as little as possible in RAM:

- **Streaming `_id` enumeration**: IDs are read with a cursor (`batchSize=1000`) and written to disk as JSONL line-by-line — never materialized as a full array. On full backups no in-memory `Set` is built at all.
- **Per-collection snapshot files**: Each collection's IDs go into its own `ids/<ID>/<collection>.jsonl` so neither writing nor reading needs to load all collections at once. Delete detection streams the previous run's file through `readline`, holding only the *current* collection's IDs in memory at any time.
- **Streaming upsert detection**: The list of changed `_id`s for the tracking file is also collected via cursor iteration, not `.toArray()`.
- **Pause between collections**: A 300 ms `sleep` after each collection gives MongoDB's WiredTiger cache time to evict pages and write checkpoints before the next dump starts.
- **Hard Node heap cap (Docker)**: The `Dockerfile` launches Node with `--max-old-space-size=1024`, so a runaway backup OOMs Node cleanly with a stack trace instead of letting the Linux OOM killer pick `mongod`.

If you still see `mongod` getting killed during the initial full run, the bottleneck is almost certainly MongoDB's own WiredTiger cache. Pin it explicitly in your `mongod.conf`:

```yaml
storage:
  wiredTiger:
    engineConfig:
      cacheSizeGB: 2   # adjust to ~50% of the container's memory limit
```

This is critical in Docker setups, where MongoDB's default cache sizing can ignore the container's cgroup limit and try to claim half the host's RAM.

---

## License
MIT
