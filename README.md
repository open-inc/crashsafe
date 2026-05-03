# MongoDB CrashSafe

A robust Node.js daemon + CLI for **incremental MongoDB backups** using native `mongodump`. Designed for **standalone MongoDB with independent collections** (sensor / time-series workloads); not for cross-collection-consistent replica-set backups — see [*Consistency Model & Scope*](#consistency-model--scope). It perfectly preserves BSON types (ObjectIDs, Dates, etc.) and features a premium Web Dashboard for monitoring and manual triggers.

## Features

- **BSON Native**: Uses `mongodump` and `mongorestore` for maximum reliability and type safety.
- **Incremental Logic**: Efficiently backs up only changed documents using `updatedAt` timestamps.
- **Deletion Tracking**: Tracks both **per-document deletions** and **whole-collection drops** via lightweight `_id` snapshots, so a wipe-and-restore reflects deletions accurately.
- **Append-Only Mode**: Optional fast path for hot append-only workloads (sensor streams) — skips per-document ID enumeration. Configurable per-database. See [*Append-Only Mode*](#append-only-mode).
- **Integrity Verification**: SHA-256 checksums per file are written at backup time and re-verified on demand or on a scheduled cron. Catches bit-rot, half-written dumps, and tampering before they bite at restore time. See [*Integrity Verification*](#integrity-verification).
- **Destructive-action guardrails**: Pre-flight chain validation aborts every destructive restore *before* any collection is dropped if the chain on disk is incomplete or corrupt. The destructive `dropExisting` flag is explicit on every code path (no longer inferred from `type:'full'`). The daemon refuses to start at all if `DB_DATA` or `DB_PARSE` points at a MongoDB system database (`admin`, `config`, `local`). Manifest paths used during restore are safe-joined against the backup tree, so a tampered manifest can't escape via `../`. UI restores additionally require a typed confirmation phrase. See [*Restore Safety*](#restore-safety).
- **Sidecar restore mode**: Optional `mode: 'sidecar'` replays the entire chain into a shadow database, then swaps it onto the live names with one atomic `renameCollection` per coll. A failure during replay leaves the live DB byte-for-byte unchanged. Costs roughly 2× disk during the restore window; default mode stays `direct`. See [*Restore Strategies*](#restore-strategies).
- **Crash-safe manifest**: The chain index (`manifest.json`) is written via fsync + atomic rename, so a power cut or SIGKILL mid-write never leaves a torn manifest behind.
- **Credential-safe logs**: The MongoDB connection string is automatically redacted everywhere it could land in a log line — including `mongodump`/`mongorestore` failure stderr and `error.cmd`. Setting `LOG_LEVEL=debug` is safe for production.
- **Dry-run preview**: Both `backup` and `restore` accept `--dry-run` to print the plan (which collections, which chain, which mongorestore commands) without writing files or touching MongoDB beyond read queries. Useful before any destructive operation or to estimate work.
- **Separate restore destination**: `OPENINC_MONGO_BACKUP_RESTORE_URI` lets you replay a backup into a different cluster (e.g. a sandbox) for verification, without touching production.
- **Web Dashboard**: Premium status page at `http://localhost:3000` to monitor health, trigger manual runs, and run integrity checks.
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
| `OPENINC_MONGO_BACKUP_URI` | ✅ | — | MongoDB connection string used for backups (read source). |
| `OPENINC_MONGO_BACKUP_RESTORE_URI` | ❌ | = `URI` | Optional separate destination for restores. Set this to a sandbox cluster's URI to test a restore without touching production. Defaults to `URI` so existing setups are unchanged. |
| `OPENINC_MONGO_BACKUP_DB_DATA` | ⚠️ | — | Name of the data database. Refused at startup if it equals `admin`/`config`/`local`. |
| `OPENINC_MONGO_BACKUP_DB_PARSE` | ⚠️ | — | Name of the Parse server database. Same system-DB refusal applies. |
| `OPENINC_MONGO_BACKUP_DIR` | ❌ | `./backups` | Root directory for backups |
| `OPENINC_MONGO_BACKUP_CRON` | ❌ | `0 * * * *` | Cron expression |
| `OPENINC_MONGO_BACKUP_UI_PORT` | ❌ | `3000` | Port for the Web Dashboard |
| `OPENINC_MONGO_BACKUP_COLLECTION_PREFIX` | ❌ | `sensors---` | Filter for data collections |
| `OPENINC_MONGO_BACKUP_SENSOR_CONFIG_COLLECTION` | ❌ | `config` | Name of the data DB's config collection. Always fully tracked — never affected by Append-Only Mode. |
| `OPENINC_MONGO_BACKUP_UPDATED_AT_FIELD` | ❌ | `updatedAt` | Field name for change detection |
| `OPENINC_MONGO_BACKUP_AUTH_USER` | ❌ | — | Username for the dashboard's HTTP Basic Auth. Set together with `AUTH_PASSWORD` to enable auth; leave both unset to disable. |
| `OPENINC_MONGO_BACKUP_AUTH_PASSWORD` | ❌ | — | Password for the dashboard's HTTP Basic Auth. |
| `OPENINC_MONGO_BACKUP_APPEND_ONLY_DATA` | ❌ | `false` | Append-only mode for the data DB. Skips delete detection on incrementals — much faster for hot sensor streams, but **deletions are not captured**. The config collection is exempt. See *Append-Only Mode* below. |
| `OPENINC_MONGO_BACKUP_APPEND_ONLY_PARSE` | ❌ | `false` | Same as above, for the Parse DB. |
| `OPENINC_MONGO_BACKUP_VERIFY_CRON` | ❌ | — | Cron expression for scheduled integrity verification (e.g. `0 4 * * *` for daily at 04:00). Empty / unset = no scheduled verify, only on-demand. Failures log at `error` level with the message `Scheduled verify found corruption` — wire that into your alerting. |
| `OPENINC_MONGO_BACKUP_VERIFY_DEEP` | ❌ | `false` | When the scheduled verify runs, also run `gunzip -t` over every dump file. Catches the valid-hash-but-broken-gzip case. Slower (every `*.gz` is decompressed end-to-end). |

---

## Usage

### Web Dashboard
Start the daemon and visit `http://localhost:3000`. You can monitor recent runs and trigger manual backups, restores, or integrity checks per database.

The dashboard is unauthenticated by default. To require login, set `OPENINC_MONGO_BACKUP_AUTH_USER` **and** `OPENINC_MONGO_BACKUP_AUTH_PASSWORD` — the server then enforces HTTP Basic Auth on every route (UI + API), and the browser shows its native login dialog. Setting only one of the two is rejected at startup, so the daemon can't silently boot without auth. Run the dashboard behind HTTPS (e.g. Coolify's reverse proxy) since Basic Auth credentials are base64-encoded, not encrypted.

### HTTP API

The same daemon process exposes a JSON API on the same port. All routes accept Basic Auth identical to the dashboard. Mutating endpoints return `202 Accepted` immediately and run the work in the background — poll `GET /api/status` to observe progress and completion.

| Method | Path | Body | Purpose |
|---|---|---|---|
| `GET` | `/api/status` | — | Full state: scheduler, last runs (scheduled + manual), last restore, last verify, in-flight operation, per-DB backup history, effective config |
| `POST` | `/api/trigger/backup` | `{ type: 'incremental' \| 'full', target?: 'data' \| 'parse' \| 'all' }` | Trigger a backup |
| `POST` | `/api/restore/confirm` | `{ target?: 'data' \| 'parse' \| 'all' }` | Issue a single-use confirmation token for a destructive restore against this target. The response is `{ token, expiresAt }`. Token TTL is 60 s and the token is bound to the requested target. Required before any destructive `/api/trigger/restore` call. |
| `POST` | `/api/trigger/restore` | `{ type: 'full' \| 'incremental', target?, backupId?, sinceId?, dropExisting?: boolean, mode?: 'direct' \| 'sidecar', confirmToken?: string, verifyChecksums?: boolean }` | Trigger a restore. Any destructive operation (`dropExisting:true` or `mode:'sidecar'`) requires a fresh `confirmToken` from `/api/restore/confirm`. `mode:'sidecar'` replays into a shadow DB and swaps on success — see [*Restore Strategies*](#restore-strategies). `verifyChecksums:true` runs the deep pre-flight before any drop. |
| `POST` | `/api/trigger/verify` | `{ target?, backupId?, deep?: boolean }` | Trigger an integrity check. `deep:true` also runs `gunzip -t` over every `*.gz`. |

Example: trigger a paranoid Restore-to-Point against the data DB:

```bash
# Step 1: get a single-use confirm token for the destructive operation
TOKEN=$(curl -s -X POST http://localhost:3000/api/restore/confirm \
  -H 'Content-Type: application/json' \
  -d '{"target":"data"}' | jq -r .token)

# Step 2: trigger within 60 s using the token
curl -X POST http://localhost:3000/api/trigger/restore \
  -H 'Content-Type: application/json' \
  -d "$(jq -nc --arg t "$TOKEN" '{type:"full",target:"data",backupId:"2026-05-03T08:00:00.000Z",dropExisting:true,verifyChecksums:true,confirmToken:$t}')"
# 202 Accepted; restore runs in background. Poll /api/status for completion.
```

Note: the API does **not** require the typed-`do it` confirmation that the dashboard enforces — that's a UI-only fat-finger guard. If you wire crashsafe into automation, build your own gating around these endpoints.

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
# like it did at the chosen backup entry (point-in-time restore).
# Always runs a pre-flight chain check first; aborts before the drop if the
# chain is broken on disk. The --yes-i-am-sure-this-wipes flag is mandatory
# for any --dropExisting run; pass the actual target DB name (or "all").
openinc-crashsafe restore <ID> --full --dropExisting --yes-i-am-sure-this-wipes <dbName>

# Same as above, but also re-hashes every file in the chain against stored
# SHA-256s before the drop. Slow on large chains, but catches silent on-disk
# corruption that the cheap pre-flight (existence + parseability) wouldn't see.
# Recommended for any --dropExisting on a large/old chain.
openinc-crashsafe restore <ID> --full --dropExisting \
  --yes-i-am-sure-this-wipes <dbName> --verify-checksums

# Sidecar mode: replay into a shadow DB, swap on success. Live DB is
# untouched if anything fails during replay. Costs 2× disk during the run.
# Required for paranoid PITRs against a healthy live DB.
openinc-crashsafe restore <ID> --full --mode=sidecar \
  --yes-i-am-sure-this-wipes <dbName>

# Plan only: list what a real run would do, write nothing. Works for both
# backup and restore. Restore --dry-run still runs the read-only pre-flight,
# so chain corruption surfaces here before you commit to a real run.
openinc-crashsafe backup --dry-run
openinc-crashsafe restore <ID> --full --dropExisting --dry-run

# Advanced: replay every entry from <fromID> forward without dropping (rarely
# useful — only when you're certain the current data matches the state right
# before <fromID>)
openinc-crashsafe restore <toID> --since <fromID>

# Verify backup integrity by re-hashing files against stored SHA-256s.
# Exit codes: 0 = all good, 1 = corruption found, 2 = legacy entries without
# checksums (warning, not failure) — usable in cron pipes / CI.
openinc-crashsafe verify
openinc-crashsafe verify --target=data --id=<backupId>   # narrow scope
openinc-crashsafe verify --deep                          # also gunzip -t every dump
openinc-crashsafe verify --json                          # machine-readable output
```

---

## Backup Format

Backups are stored in `backups/<dbType>/<ID>/`.

- **`<ID>/`**: A directory containing `mongodump` BSON archives (per-collection `*.bson.gz` + `*.metadata.json.gz`).
- **`<ID>.tracking.json`**: Per-entry change log. Records deleted document `_id`s, upserted `_id`s, and whole-collection drops (`{op: 'drop', collection: '...'}`) for precise incremental replay.
- **`ids/<ID>/<collection>.jsonl`**: Per-collection `_id` snapshot (one EJSON-encoded `_id` per line). Used for delete detection on the next run **and** for collection-drop detection (a collection that wrote a marker last run but is missing from `listCollections` this run is recorded as a drop).
- **`manifest.json`**: The index of all backup runs. Each entry records the file paths above plus `size`, `trigger`, `finishedAt`, and `checksums` (SHA-256 per file, split into `dump` / `tracking` / `ids` so verify failures can be triaged by impact). Written via fsync + atomic rename — see [*Crash-Safe Manifest*](#crash-safe-manifest).

### How it works
1. **Change Detection**: For each collection we stream all `_id`s from the DB and compare against the previous run's snapshot file to find **deleted** documents. After the per-collection loop, we compare the *set* of collections seen this run to the previous JSONL directory — anything missing is recorded as a **collection-level drop**.
2. **Incremental Dump**: We run `mongodump --query` with a **half-open time window** — `updatedAt > lastRunDate AND updatedAt <= thisRunStartDate`. The upper bound matters: without it, a write that happens during a running backup can land in the dump of a late-iterated collection but not in an early-iterated one, producing a per-collection inconsistency at the manifest's id timestamp. With it, every collection in the same run sees the exact same time window, and writes that happen *during* the run flow cleanly into the next inc.
3. **Checksum**: After all files are written, every dump file, the tracking file, and every `_id`-snapshot file is SHA-256-hashed and the digests are recorded in the manifest entry. Best-effort — a hashing failure logs but doesn't fail the backup.
4. **Restore**: We pre-flight-validate the chain (existence + parseability of every file, optionally also re-hash). On any chain issue, the restore is aborted *before* anything is dropped. Otherwise: delete any modified/deleted IDs via the JavaScript driver, drop any collections recorded as dropped, then run `mongorestore` to upsert the new data.

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

## Consistency Model & Scope

crashsafe is designed for **standalone MongoDB** with **independent collections** — the canonical use case is sensor / time-series workloads where one collection per stream is the norm and there are no cross-collection references. It is **not** a tool for replica-set point-in-time-consistent backups.

### Why this matters

`mongodump` is invoked per collection in a serial loop. A full backup of N collections therefore sees each collection at a slightly different wall-clock moment — collection 1 is dumped at T₁, collection 2 at T₂, and so on. If your application performs **cross-collection writes during a backup** (e.g. inserts a row in `orders` and a row in `order_items` in the same logical operation), the backup can capture one side and miss the other. On restore, you'd see a referentially inconsistent dataset (an order_item pointing at an order that doesn't exist, or vice versa).

| Schema shape | Cross-collection inconsistency possible? |
|---|---|
| Sensor / time-series (one collection per stream, no cross-references) | ❌ No — collections are independent |
| Audit logs, event streams, append-only logs | ❌ No |
| Relational (`orders` ↔ `order_items`, foreign-key-style references) | ✅ Yes — under load during a backup |

### Why we don't use `--oplog`

`mongodump --oplog` against a replica set captures the oplog cursor at backup start and includes the oplog tail; `mongorestore --oplogReplay` then applies every write that happened during the dump and yields a real point-in-time-consistent snapshot. That's the correct fix for cross-collection consistency on a replica set — but crashsafe doesn't use it, by design:

- The backup architecture is **per-collection** (the inner loop calls `mongodump --db X --collection Y` once per collection). `--oplog` only makes sense as part of a single bulk-database dump, which would conflict with the per-collection `--query` incremental optimization and the per-collection id-snapshot machinery.
- The target use case (sensor streams) doesn't need it.
- Replica sets are usually run with proper backup tooling (Cloud Manager, Ops Manager, storage-level snapshots), so adding a half-baked `--oplog` path would be worse than not having it.

### What to do if you need cross-collection consistency

If your schema relies on referential consistency across collections, **crashsafe is the wrong fit**. Use one of:

- `mongodump --oplog` directly via cron, plus your own retention/verify story.
- **MongoDB Cloud Manager / Ops Manager** for managed PITR backups against a replica set.
- **Storage-level snapshots** (LVM, EBS, ZFS) of the MongoDB data directory while a secondary is `fsyncLock`ed.

---

## Restore Safety

Two layers of guardrail protect the live database from being wiped on a broken chain or by an accidental click.

### Pre-flight chain validation (always on)

Every restore — whether triggered from the UI, the CLI, or the API — first runs a pre-flight pass over the entire chain it is about to replay. The pass checks:

- Every entry's dump directory exists on disk (where the entry has one).
- Every entry's tracking file exists and parses as EJSON.
- Optionally (`--verify-checksums`): every file's stored SHA-256 still matches what's on disk.

If any check fails, the restore is **aborted with a clear error before anything is dropped**. The live database is untouched. The error message lists every issue found across the chain in one go, so the operator can fix the backup tree and retry.

The cheap pre-flight (existence + parseability) costs effectively nothing and runs unconditionally. The expensive deep pre-flight (`--verify-checksums`) re-hashes every file and is opt-in because on a long chain it can take many minutes — but it's the only way to catch silent on-disk bit-rot before the drop.

```bash
# CLI
openinc-crashsafe restore <ID> --full --dropExisting --verify-checksums

# API
POST /api/trigger/restore
Content-Type: application/json
{ "type": "full", "target": "data", "backupId": "<ID>", "verifyChecksums": true }
```

The auto-test in `local-test/` includes a regression test (Phase P) that deliberately corrupts a tracking file in the chain, fires a Restore to Latest, and asserts the live DB is byte-for-byte unchanged afterwards.

### Explicit `dropExisting` flag

The destructive part of a restore — "drop every collection in the target database before replaying" — is its own boolean, never inferred from anything else. The CLI gates it behind `--dropExisting`, the API requires `dropExisting: true` in the body, the dashboard sends it explicitly when the user clicks "Restore to Latest" or "Restore to Point". A client that sends `type: 'full'` alone gets a chain replay layered on top of existing data — useful for forward-rolling a known-good state, harmless if you didn't mean to wipe.

This separation costs nothing and closes the case where a misconfigured client (or a tutorial copy-pasted somewhere) drops a live database just because it asked for a "full restore."

### System-database refusal

`OPENINC_MONGO_BACKUP_DB_DATA` and `OPENINC_MONGO_BACKUP_DB_PARSE` are validated at access time. If either points at a MongoDB system database — `admin`, `config`, or `local` — the daemon throws on first access and refuses to run. Rationale: backing up the auth database (`admin`) and restoring it into a different cluster could nuke that cluster's user accounts; `config` holds shard metadata; `local` is per-replica state. None of these belong in a logical-backup pipeline. If you really need to back them up, use `mongodump` directly.

### Manifest path-traversal guard

Every path read out of a manifest entry — `entry.file`, `entry.trackingFile`, `entry.idDir`, and the per-file checksum keys — is run through `safeJoin(backupDir, ...)` before it's opened. A manifest tampered to contain `"../../etc"` is rejected with a clear error before any file is touched, in both the verify and the restore paths.

### Typed-confirmation modal (UI)

Every destructive restore button in the dashboard (Restore to Latest *and* Restore to Point) opens a confirmation modal that only enables its OK button after the operator types the literal phrase **`do it`** into an input field. The phrase is case-insensitive and trimmed. Cancel + Esc + clicking outside the modal all dismiss it without action.

After the user confirms, the dashboard fetches a single-use confirm token from `/api/restore/confirm` and includes it in the trigger call — the typed phrase is the human gate, the token is the API gate. Both are required for destructive operations from the UI.

### Server-side confirmation tokens (API)

For destructive restores hitting the API directly (`dropExisting: true`), the server requires a fresh **confirm token** that's bound to the target. Without one, `POST /api/trigger/restore` rejects with 403.

```
POST /api/restore/confirm    body: { target: 'data' }
                             →   { token, expiresAt }     # TTL 60 s, single-use

POST /api/trigger/restore    body: { type:'full', target:'data', dropExisting:true,
                                     confirmToken: '<token>', ... }
```

The token is single-use (consumed on the trigger call regardless of outcome) and bound to the exact target — a token issued for `data` cannot be used to wipe `parse`. Tokens are kept in-memory only, so a daemon restart invalidates every outstanding one. The flow forces a two-round-trip pattern even for scripted callers: a stray single curl POST cannot wipe the live DB.

### CLI typed-name confirmation

The CLI uses a parallel mechanism: every `--dropExisting` run requires `--yes-i-am-sure-this-wipes <name>`, where `<name>` must match the target DB's actual configured name (or the literal `all` when `--target=all`).

```bash
# Refused — no confirmation
openinc-crashsafe restore 2026-05-03T08:00:00.000Z --full --dropExisting

# Refused — wrong DB name
openinc-crashsafe restore 2026-05-03T08:00:00.000Z --full --dropExisting --yes-i-am-sure-this-wipes wrongname

# Accepted — name matches the configured target
openinc-crashsafe restore 2026-05-03T08:00:00.000Z --full --dropExisting --yes-i-am-sure-this-wipes mydatadb
```

The CLI doesn't round-trip a token because there's no HTTP layer — the typed name is itself the proof of intent. `--dry-run` skips this gate because nothing destructive runs.

### Restore Strategies

Every destructive restore can run in one of two modes. The choice is per-call (CLI `--mode`, API body `mode`, or the dropdown in the dashboard's typed-confirmation modal). Default is `direct`, matching historical behavior.

| Property | `direct` (default) | `sidecar` |
|---|---|---|
| **Disk during restore** | 1× backup size | **2× backup size** (live + shadow) |
| **Wall-clock time** | Single replay | Replay + brief swap (basically same) |
| **Live DB state if replay fails** | Half-restored — operator must re-run the same restore | **Untouched** — ready for retry, no half-state |
| **Live collections that aren't in the backup** | **Wiped** (drop-all happens before replay) | **Preserved** — only collections in the backup are swapped |
| **MongoDB calls during destruction** | 1× drop per live collection | 1× `renameCollection` per chain collection (atomic per coll) |
| **Use when** | Disaster-recovery — live DB is already gone or unusable | PITR roll-back of a healthy live DB; you want safety over speed |

**Direct (default)** is the historical behavior: drop every live collection, then replay the chain into the now-empty DB. Fast and disk-efficient. The catch is the failure mode — a network drop, OOM, or mongorestore bug halfway through the replay leaves a half-restored DB that's neither old nor new. Recovery requires re-running the restore (which works fine because the backup tree on disk is read-only during a restore — any failure is recoverable from the same chain).

**Sidecar** runs the whole replay into a shadow database `<liveDb>__crashsafe_restore_<runId>`, never touching the live DB. Once the entire chain has replayed successfully, each shadow collection is renamed onto the live DB via `db.adminCommand({renameCollection, dropTarget: true})` — a single mongo command per collection, atomic from MongoDB's view. Failure modes:

- **Failure during replay** — sidecar gets dropped, live DB byte-for-byte unchanged. Retry is a re-run.
- **Failure during the swap phase** — partial state. The sidecar still holds the unswapped collections. The error log lists which collections succeeded and which didn't, with concrete `db.adminCommand({renameCollection, ...})` calls to finish the swap manually. This is the rarer case; the swap window is brief because rename is a metadata-only operation, no data movement on the same shard.

Sidecar mode auto-cleans orphan shadow databases left by previous failed runs (matched by the `__crashsafe_restore_` prefix) before starting a new one. So even after a crash, the next sidecar run begins from a clean slate.

**Important constraints:**

- Sidecar requires `--full`. It always replays the whole chain — there's no atomic-swap semantic for partial replay.
- Sidecar ignores `--dropExisting` because the swap IS the destruction. The typed-confirm gate (UI) and confirm token (API) still apply because it's still destructive overall.
- Sidecar requires the MongoDB user to have permission to issue cross-database `renameCollection` commands (admin role on the destination cluster).
- The shadow DB shows up in `listDatabases()` during the restore window. If you have monitoring on `databases`-changed events, expect a brief noise pulse.

```bash
# CLI
openinc-crashsafe restore <ID> --full --mode=sidecar \
  --yes-i-am-sure-this-wipes <dbName>

# API (after /api/restore/confirm — same token flow as direct mode)
curl -X POST http://localhost:3000/api/trigger/restore \
  -H 'Content-Type: application/json' \
  -d "$(jq -nc --arg t "$TOKEN" '{type:"full",target:"data",backupId:"<ID>",mode:"sidecar",confirmToken:$t}')"
```

The dashboard's typed-confirm modal has a dropdown that lets the operator pick the mode at click-time, defaulting back to `direct` after every modal open.

### Restore destination (separate URI)

`OPENINC_MONGO_BACKUP_RESTORE_URI` is an optional second connection string that, when set, redirects every restore destination — `mongorestore`'s target, the pre-replay `deleteMany` operations, and the destructive `drop` on `dropExisting` — to a different cluster. The backup URI (`OPENINC_MONGO_BACKUP_URI`) keeps reading from the source as before.

```env
# Backups continue to read from the production cluster
OPENINC_MONGO_BACKUP_URI=mongodb://prod-host/?authSource=admin

# Restores go to a sandbox — production is never written by a restore
OPENINC_MONGO_BACKUP_RESTORE_URI=mongodb://sandbox-host/?authSource=admin
```

Use cases:
- Periodic restore drills against a sandbox without risk to production.
- Migrating: one-time redirect during a cutover window.
- Compliance: keep a read-only "no-restores" credential on production while still letting the daemon restore elsewhere.

The destination cluster's DB names must match what's in the dump (`mongorestore` writes into the DB named in the dump's directory layout). If you need rename semantics, that's outside crashsafe — use `mongorestore --nsTo` directly.

When unset (default), restores go to the same URI as backups, identical to historical behaviour.

### What is *not* protected

Pre-flight cannot detect:

- A network drop *during* `mongorestore` after the chain was validated. If you start a 10-minute restore and the connection to MongoDB dies at minute 5, you have a half-restored database — pre-flight already passed by then.
- Logical corruption of the source data that was faithfully captured at backup time. If your application wrote bad data and crashsafe dumped it, restore reproduces it.
- A misconfigured `OPENINC_MONGO_BACKUP_DB_DATA` pointing at the wrong database. Pre-flight only checks the backup tree, not whether you mean to restore *into* the database you've configured.

For the network-drop case, a sidecar-restore pattern (restore into a shadow DB, swap on success) is the standard mitigation but is not yet implemented. Open a discussion if you need it.

---

## Integrity Verification

CrashSafe writes SHA-256 hashes of every backup file into the manifest entry at write time, and offers an explicit verify operation that re-hashes the on-disk files and compares.

### What gets hashed

Each backup entry's `checksums` map is split into three sections so failures can be triaged by impact:

| Section | What's in it | Failure means |
|---|---|---|
| `dump` | Every file under `<ID>/` (`mongodump` output: `*.bson.gz`, `*.metadata.json.gz`, `prelude.json`) | This entry's dump can't be replayed → restore is blocked |
| `tracking` | The single `<ID>.tracking.json` file | Inc-replay can't apply this entry's deletes/upserts → chain is broken at this entry |
| `ids` | Every `ids/<ID>/<collection>.jsonl` | Next inc can't detect deletes against this entry → next inc is incomplete |

Legacy entries written before checksum tracking existed have no `checksums` field. Verify reports them as **`no-baseline`** — a warning, not a failure (CLI exit 2).

### Triggering a verify

| Surface | How |
|---|---|
| Web Dashboard | Per-DB "Verify Integrity" button. Result lands on the "Last Verify" status card. |
| CLI | `openinc-crashsafe verify` (see CLI section above for flags) |
| API | `POST /api/trigger/verify` with body `{ target?, backupId?, deep? }`. Async, returns 202; poll `/api/status` for `lastVerify`. |

### `--deep` mode

Without `--deep`, verify only re-hashes files. A file whose contents have been silently truncated *but where the SHA-256 was recomputed over the truncated bytes* would still pass. This can happen if `mongodump` itself was killed mid-write and a later run accidentally re-hashed the broken output.

`--deep` runs `gunzip -t` over every `*.gz` dump file in addition to the SHA check — it tests that the gzip stream is structurally complete and decompresses cleanly without actually expanding the data to disk. Slow (every file is read end-to-end and decompressed in memory) but catches the gzip-broken-but-hash-matches case.

### Scheduled verify (built-in)

Set `OPENINC_MONGO_BACKUP_VERIFY_CRON` to enable an automatic verify on a separate cron from the backup cron:

```env
# Daily at 04:00 — separate from the backup cron, so the two never overlap timing-wise
OPENINC_MONGO_BACKUP_VERIFY_CRON=0 4 * * *
# Optional: also run gunzip -t over every dump (slower, catches more)
OPENINC_MONGO_BACKUP_VERIFY_DEEP=false
```

Empty / unset = no scheduled verify (the on-demand UI/CLI/API stays available).

The scheduled verify shares the same lock as backups and restores, so there's no risk of overlap with a long-running backup or a restore-in-progress — the scheduled run will skip itself with a `Scheduled verify skipped` warning if anything else is in flight.

**Log lines for alerting:**

| Outcome | Log level | Message | Use this for |
|---|---|---|---|
| Found real corruption (`corrupt > 0` or `manifestErrors > 0`) | `error` | `Scheduled verify found corruption` (with `summary` + `sampleDetails` fields) | Page-someone alerts. The most important signal. |
| Legacy entries without checksums | `warn` | `Scheduled verify completed with legacy entries (no baseline)` | Informational. Once your retention has aged out the legacy entries this stops firing. |
| All clean | `info` | `Scheduled verify completed clean` | Heartbeat — absence of recent log line = scheduled verify isn't running. |
| Skipped (lock held) | `warn` | `Scheduled verify skipped (another operation in progress)` | Watch for runaway frequency — if a verify keeps skipping, something else is hogging the lock. |
| Crashed | `error` | `Scheduled verify failed` | Bug in verify itself; rare. |

The dashboard's "Last Verify" status card shows the most recent result regardless of how it was triggered (scheduled, manual, or API).

### External cron alternative

If you'd rather drive verification from a host-level scheduler (CI runner, Kubernetes CronJob, plain crontab) instead of the daemon's built-in cron, the CLI supports it directly:

```bash
# Example: nightly deep verify, alert on any non-zero exit
0 3 * * * /usr/local/bin/openinc-crashsafe verify --deep --json > /var/log/crashsafe-verify.log || /usr/local/bin/alert
```

Exit code 1 = real corruption (page somebody). Exit code 2 = legacy entries (informational; harmless if you never plan to restore those entries). Exit code 0 = clean.

---

## Crash-Safe Manifest

`manifest.json` is the index that ties dump files into chains. If it gets torn (a partial write from a power cut or SIGKILL mid-write), the daemon can't read its own state and won't start.

CrashSafe writes the manifest using a four-step durable pattern, every time:

1. Write the new content to a sibling `manifest.json.tmp`.
2. `fsync(2)` the tmp file so its bytes are physically on disk.
3. `rename(2)` `manifest.json.tmp` → `manifest.json`. POSIX rename within one directory is **atomic** — concurrent readers see either the old file or the new one, never a torn read.
4. `fsync(2)` the parent directory so the rename itself is durable across kernel-level crashes.

A SIGKILL or power cut at any point in this sequence leaves either the previous manifest or the new manifest fully intact on disk — there is no in-between. Test coverage in `test/manifest_atomic.test.js` includes a repeated SIGKILL-during-write regression test that fires a child process writing a 5000-entry manifest in a tight loop and `SIGKILL`s it at randomized points; every trial must leave a parseable manifest behind.

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
- **Orphan cleanup on startup**: a backup that crashed *after* writing some files but *before* updating the manifest leaves a slug-named directory (and possibly a `<slug>.tracking.json` and `ids/<slug>/`) behind. On the next daemon start, those are detected (slug-shaped names that don't appear in any manifest entry) and deleted. Only entries older than one hour are touched, leaving a margin against any in-flight backup that started just before the daemon boot.

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

**2. Auto-Test (correctness verification).** A ≈60-step end-to-end test that drives crashsafe through a deterministic backup/restore cycle and verifies data integrity by **canonical-EJSON SHA-256 fingerprinting** every collection at each state (BSON-typed values like `Date`, `ObjectId`, `Decimal128` are compared verbatim — JSON.stringify-style hashing would let a `Date → string` regression slip through, so the harness uses `EJSON.stringify(doc, {relaxed:false})` with recursively-sorted keys). Covers:

- Full backup → modify → incremental backup → Restore to Latest → fingerprint must match modified state
- Restore to Point at the Full → fingerprint must match initial state
- Excluded collection (`not_a_sensor`) is absent after restore
- **PITR to a specific Inc** must yield that exact state, not roll-forward to latest (regression test for the chain semantics)
- **A second Full creates a fresh checkpoint** that older Fulls don't bleed into (regression test for `getChainUpTo`)
- **Config-collection deletes are honoured** by restore (regression test for the historical "deleted config doc resurrects" bug)
- **Collection-drop tracking**: a sensor collection is created, backed up, then dropped, then backed up again — Wipe & Restore must NOT silently re-create the dropped collection
- **Parse-DB delete tracking**: `parse/items` deletes survive a wipe-and-restore (canonical positive test for non-append-only delete tracking — the data DB is in append-only mode in the default compose)
- **Index preservation**: the `updatedAt` index round-trips through dump+restore on every backed-up collection
- **Integrity verify happy path → bit-flip → corruption-detected → repair → clean** — proves verify both passes on a fresh chain and catches a 1-byte tampering on a real `*.bson.gz`
- **Pre-flight guardrail (Phase P)**: deliberately corrupts a real tracking file in the chain, fires Restore to Latest, and asserts the live database is byte-for-byte unchanged afterwards. The destructive guardrail is the most important behavior in the whole tool — this test is its load-bearing regression
- Manifest counts, total size tracking, no stranded lock at the end

Click <strong>Run Auto-Test</strong>, watch the live step list and log, and read the green/red summary at the end. Total runtime ≈ 3 minutes.

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
