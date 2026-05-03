# MongoDB CrashSafe

A robust Node.js daemon + CLI for **incremental MongoDB backups** using native `mongodump`. It perfectly preserves BSON types (ObjectIDs, Dates, etc.) and features a premium Web Dashboard for monitoring and manual triggers.

## Features

- **BSON Native**: Uses `mongodump` and `mongorestore` for maximum reliability and type safety.
- **Incremental Logic**: Efficiently backs up only changed documents using `updatedAt` timestamps.
- **Deletion Tracking**: Automatically tracks and replays document deletions using lightweight ID snapshots.
- **Web Dashboard**: Premium status page at `http://localhost:3000` to monitor health and trigger manual runs.
- **Scheduled**: Runs as a background daemon with a configurable cron schedule.
- **Point-in-Time Restore**: Drop and rebuild the database to look exactly like it did at any past backup entry.

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
| `OPENINC_MONGO_BACKUP_AUTH_USER` | ❌ | — | Username for the dashboard's HTTP Basic Auth. Set together with `AUTH_PASSWORD` to enable auth; leave both unset to disable. |
| `OPENINC_MONGO_BACKUP_AUTH_PASSWORD` | ❌ | — | Password for the dashboard's HTTP Basic Auth. |
| `OPENINC_MONGO_BACKUP_APPEND_ONLY_DATA` | ❌ | `false` | Append-only mode for the data DB. Skips delete detection on incrementals — much faster for hot sensor streams, but **deletions are not captured**. The config collection is exempt. See *Append-Only Mode* below. |
| `OPENINC_MONGO_BACKUP_APPEND_ONLY_PARSE` | ❌ | `false` | Same as above, for the Parse DB. |

---

## Usage

### Web Dashboard
Start the daemon and visit `http://localhost:3000`. You can monitor recent runs and trigger manual backups or restores per database.

The dashboard is unauthenticated by default. To require login, set `OPENINC_MONGO_BACKUP_AUTH_USER` **and** `OPENINC_MONGO_BACKUP_AUTH_PASSWORD` — the server then enforces HTTP Basic Auth on every route (UI + API), and the browser shows its native login dialog. Setting only one of the two is rejected at startup, so the daemon can't silently boot without auth. Run the dashboard behind HTTPS (e.g. Coolify's reverse proxy) since Basic Auth credentials are base64-encoded, not encrypted.

### CLI Commands

```bash
# Start the daemon (cron + dashboard)
openinc-crashsafe

# Run a manual incremental backup (changed docs since the last entry)
openinc-crashsafe backup

# Run a manual full backup (re-dumps every collection from scratch — new checkpoint)
openinc-crashsafe backup --full

# List the backup chain
openinc-crashsafe list

# Apply only the latest entry's diff on top of the current data (no drop)
openinc-crashsafe restore

# DESTRUCTIVE: drop all collections, then rebuild the database to look exactly
# like it did at the chosen backup entry (point-in-time restore)
openinc-crashsafe restore <ID> --full

# Advanced: replay every entry from <fromID> forward without dropping (rarely
# useful — only when you're certain the current data matches the state right
# before <fromID>)
openinc-crashsafe restore <toID> --since <fromID>
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

## Backup & Restore Model

### The chain

Every backup entry is one link in a chain stored in `manifest.json`:

```
Full ─ Inc ─ Inc ─ Inc ─ Full ─ Inc ─ Inc ─ Inc ─ ...
└─────── checkpoint #1 ───┘    └────── checkpoint #2 ─────...
```

- A **Full** entry is a complete snapshot — a checkpoint. It owns the entire dataset at that moment.
- An **Incremental** entry only contains the documents that changed since the previous entry, plus a tracking file recording any deletions.
- Every Full **resets the chain**: a restore that targets any entry only needs the most recent Full at-or-before it, plus the incrementals between that Full and the target. Older Fulls are irrelevant for that restore.

### Restore actions (Web Dashboard)

Both restore buttons are **destructive**: they drop every collection in the target database, then rebuild it from the chain. Anything written after the chosen point is lost. Backup files on disk are never touched.

| Action | What it does |
|---|---|
| **Restore to Latest** | Drops the database, then replays the chain `[latest Full ≤ latest entry] → ... → latest entry`. Result: the database matches the most recent backup. |
| **Restore to Point** | Drops the database, then replays the chain `[latest Full ≤ chosen entry] → ... → chosen entry`. Result: the database matches the state captured at the chosen entry. |

In the dropdown for *Restore to Point*, every entry (Full or Incremental) is a valid target. The chain walker picks the right Full to start from automatically.

### What is *not* a restore action

- Picking an Incremental as target does **not** ignore the preceding Full. The Full is always replayed first to seed the data.
- Restoring does **not** affect collections in the target database that aren't in the dump (e.g. for the data DB, only `config` and `<prefix>*` collections are tracked — others are left alone, since they were never backed up).
- The CLI's `restore <toID> --since <fromID>` is the only non-destructive replay path. It applies entries `[fromID...toID]` on top of the current data without dropping. Use only when you are certain the current data is at the state just before `fromID`. Not exposed in the UI to avoid foot-guns.

---

## Append-Only Mode

For very high-cardinality, very hot workloads (e.g. **10,000 sensor collections each ingesting one doc per second**), the per-incremental cost of enumerating every `_id` in every collection — required to detect document deletions — quickly becomes the dominant bottleneck. A single incremental can take many hours when the actual changed-data dump is tiny.

If your workload is **append-only** (inserts only, no per-document deletions), you can opt into a faster path that skips delete detection.

### What changes

For collections in append-only mode, the per-collection incremental flow drops from:

```
1. Cursor over every _id  →  build current Set        (slow at scale)
2. Stream previous run's JSONL  →  diff for deletions (slow at scale)
3. Upsert query (updatedAt > last)                    (fast, indexed)
4. Write fresh per-collection JSONL                   (large, sequential)
5. mongodump --query of changes                       (small if changes are small)
```

…to just:

```
1. Upsert query (updatedAt > last)                    (fast, indexed)
2. mongodump --query of changes
```

No ID enumeration, no JSONL writes, no prev-snapshot reads. The disk usage of the snapshot directory stays close to zero for the active collections.

### Configuration

Two env vars enable the mode independently per database:

| Variable | Effect |
|---|---|
| `OPENINC_MONGO_BACKUP_APPEND_ONLY_DATA=true` | Append-only for the data DB |
| `OPENINC_MONGO_BACKUP_APPEND_ONLY_PARSE=true` | Append-only for the Parse DB |

Default is `false` for both — full delete tracking, current behaviour.

### Trade-off

| Operation | Default mode | Append-only mode |
|---|---|---|
| Insert (new doc with `updatedAt = now`) | ✅ Captured | ✅ Captured |
| Modify (existing doc with updated `updatedAt`) | ✅ Captured | ✅ Captured |
| **Delete a document** | ✅ Captured (chain replay removes it) | ❌ **Not captured** — doc stays in chain, restore re-introduces it |
| Drop an entire collection | ✅ Captured (collection absent on next listCollections) | ✅ Captured (same mechanism, listCollections drives the iteration) |
| Modify *without* updating `updatedAt` | ❌ Not captured (architectural limit) | ❌ Not captured (same) |

If your data is genuinely append-only (sensor readings, audit logs, event streams, time-series), the missed-deletion case rarely matters — TTL-style cleanups are intentional and you usually don't want them rolled back.

For transactional data (user records, orders, configurations) you almost always want the default mode.

### Config collection is always tracked

The `config` collection in the data DB (controlled by `OPENINC_MONGO_BACKUP_SENSOR_CONFIG_COLLECTION`, default `config`) **never** runs in append-only mode, even when `APPEND_ONLY_DATA=true`. It's always full-dumped with full delete tracking. Rationale: it's typically tiny so the overhead is negligible, and config-doc deletions are usually critical changes you want represented faithfully on restore.

### Mixed strategy

A common production pattern is **append-only for `data` only**:

```
OPENINC_MONGO_BACKUP_APPEND_ONLY_DATA=true
# OPENINC_MONGO_BACKUP_APPEND_ONLY_PARSE not set → defaults to false
```

You get the speedup on the high-volume sensor side; the smaller, transactional Parse DB keeps its full delete-aware semantics.

### Switching modes

The mode is read at the start of every backup run. You can flip it without restoring or recreating manifests, but be aware of the transition window:

- **off → on**: the next incremental simply stops writing per-collection JSONLs for the affected DB. Existing snapshots are kept on disk; subsequent runs ignore them. Harmless.
- **on → off**: the next incremental reads the previous snapshot for delete detection, but in append-only mode no snapshot was written — so the *first* post-switch incremental can't catch deletions that happened during the append-only stretch. After that incremental, full tracking resumes normally.

If you need a clean cut-over with full correctness from a known point, do a **full backup** right after switching modes. That re-establishes the snapshot and starts a fresh chain checkpoint.

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

## Local Test Environment

The repo ships with a self-contained Docker Compose setup under [`local-test/`](local-test/) that runs MongoDB, the CrashSafe daemon (built from the local source), and a small **Test Harness** web app — designed to verify and stress-test the full backup/restore cycle end-to-end on your machine.

### What it spins up

| Service | Port | Role |
|---|---|---|
| `mongodb` | `127.0.0.1:27017` | Auth-enabled Mongo 7 (`admin`/`password`) |
| `crashsafe` | `127.0.0.1:3000` | The backup daemon's dashboard (built from this repo) |
| `testapp` | `127.0.0.1:3001` | Test Harness — manual data manipulation + automated test runners |

`local-test/backups/` is bind-mounted into the crashsafe container so dumps are visible on the host filesystem.

### Starting it

```bash
cd local-test
docker compose up -d --build
```

Use `--build` whenever you change the source — the crashsafe image bakes in `src/` at build time.

Open:
- **Test Harness**: <http://localhost:3001>
- **CrashSafe Dashboard**: <http://localhost:3000>

### The Test Harness (port 3001)

Three things you can do here:

**1. Manual data manipulation.** Per-collection buttons for adding (`+1`, `+10`, `+100`, `+1k`), modifying random docs (`~1`, `~10`), deleting (`−1`, `−10`), viewing samples, and dropping. Bulk actions across all backed-up collections at the top. Use this to set up arbitrary states by hand and observe what the dashboard shows.

The harness annotates every collection with a pill: <strong>backed up</strong> (green) for collections that match the prefix filter or are the config collection, <strong>excluded</strong> (orange) for everything else. The seed includes a deliberately-named `not_a_sensor` collection in the data DB so you can verify the prefix filter visually.

**2. Auto-Test (correctness verification).** A 38-step end-to-end test that drives crashsafe through a deterministic backup/restore cycle and verifies data integrity by **SHA-256 fingerprinting** every collection at each state. Covers:

- Full backup → modify → incremental backup → Restore to Latest → fingerprint must match modified state
- Restore to Point at the Full → fingerprint must match initial state
- Excluded collection (`not_a_sensor`) is absent after restore
- **PITR to a specific Inc** must yield that exact state, not roll-forward to latest (regression test for the chain semantics)
- **A second Full creates a fresh checkpoint** that older Fulls don't bleed into (regression test for `getChainUpTo`)
- **Config-collection deletes are honoured** by restore (regression test for the historical "deleted config doc resurrects" bug)
- Manifest counts, total size tracking, no stranded lock at the end

Click <strong>Run Auto-Test</strong>, watch the live step list and log, and read the green/red summary at the end. Total runtime ≈ 2 minutes.

**3. Stress Test (volume / performance).** Configurable bulk test for measuring how the system behaves at scale.

| Field | Range |
|---|---|
| Collections | 1 – 5000 |
| Docs / collection | 1 – 100,000 |
| Mode | Seed only · Seed + Backup · Full cycle (seed + backup + restore + verify) |

Inserts are streamed in `insertMany` batches of 2500 with `ordered: false`. Verification is **count-based** (`estimatedDocumentCount` per collection, before vs after) — a hash-based verification of 250M docs would itself take many minutes. The estimate banner under the form recomputes live as you change values:

```
Total docs: 250,000,000   Raw size: 27.9 GB   Backup (gzipped): ~8.1 GB
Est. seed: ~7h   Est. full cycle: ~12h
```

Estimates are rough — actual time depends heavily on disk speed, mongodump per-collection overhead, and concurrent load. For a quick smoke test use 100 × 1k (≈ 30s); for production-ish 1000 × 50k (≈ 1–2 h); 5000 × 50k pushes the limits and runs for many hours.

The harness shows live progress for every phase: seed (collection X / Y, docs/s, ETA), backup (current DB/collection, processed/total), restore (phase: dropping → replaying → done, current entry, step counter). Final report lists every phase's duration.

### Recommended verification workflow

1. **Click "Setup Demo Data"** in the Test Harness — seeds 4 sensor collections + config + parse/items + a deliberately-excluded `not_a_sensor` collection.
2. **In the CrashSafe Dashboard, click Full Backup.** Inspect `local-test/backups/data/<id>/owdata/` — `not_a_sensor.bson.gz` should NOT be there.
3. **In the harness, modify some data** with the bulk buttons. **Click Incremental Backup** in the dashboard. Compare the two history entries' sizes.
4. **In the harness, "Wipe All Databases"** to simulate a disaster. Verify the dashboard shows zero data.
5. **In the dashboard, click Restore to Latest** on each DB card. Wait for the orange "Restore in progress" banner to clear and the green "Done · Restore completed successfully" toast to appear.
6. **Verify in the harness** that document counts match what you had pre-wipe.
7. **(Optional) Run Auto-Test** for a deterministic, fingerprint-based version of the same cycle.
8. **(Optional) Run Stress Test** at the scale that matches your production load.

### Inspecting state from the host

```bash
# Tail the daemon logs
docker compose logs -f crashsafe

# Look at the lockfile while a backup or restore is running
docker compose exec crashsafe cat /backups/.backup.lock

# Read the manifest for the data DB
docker compose exec crashsafe cat /backups/data/manifest.json | head -50

# Count docs in a collection from outside the harness
docker compose exec mongodb mongosh -u admin -p password --quiet --eval \
  'db.getSiblingDB("owdata").getCollection("sensors---temp-1").countDocuments()'
```

### Reset to a clean slate

```bash
docker compose down -v       # also drops the mongo-data volume
rm -rf backups               # wipes the bind-mounted backup dir
```

### Cron in the test compose

The test compose ships with a deliberately-rare cron (e.g. `*/45 * * * *`) so scheduled backups don't interfere with the tests. If you need to test the cron itself, edit `OPENINC_MONGO_BACKUP_CRON` in `local-test/docker-compose.yml` and re-create the container with `docker compose up -d`.

### Optional: enable Basic Auth

To verify the dashboard's HTTP Basic Auth flow, uncomment these lines in `local-test/docker-compose.yml`:

```yaml
# OPENINC_MONGO_BACKUP_AUTH_USER: "admin"
# OPENINC_MONGO_BACKUP_AUTH_PASSWORD: "test123"
```

…then `docker compose up -d` to recreate. The Auto-Test and Stress Test in the harness can pass these through if you also set `CRASHSAFE_AUTH_USER` and `CRASHSAFE_AUTH_PASSWORD` on the `testapp` service.

---

## License
MIT
