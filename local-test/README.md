# Local Test Harness

A self-contained Docker Compose setup that runs MongoDB, the CrashSafe daemon (built from this repo), and a small "test harness" web app you can use to add, modify, and delete data so you can verify the backup/restore cycle end-to-end.

## What runs

| Service | Port | Purpose |
|---|---|---|
| `mongodb` | `127.0.0.1:27017` | Auth-enabled Mongo 7 (`admin`/`password`) — local only |
| `crashsafe` | `127.0.0.1:3000` | The backup daemon's dashboard |
| `testapp` | `127.0.0.1:3001` | Web UI to inject/modify/delete data |

`backups/` is bind-mounted into the crashsafe container so you can inspect dump files directly on disk.

## Start

From this directory:
```bash
docker compose up -d --build
```

The crashsafe image rebuilds from the parent repo on each `--build`, so any code change in `src/` flows through immediately.

Then open:
- **Test harness**: <http://localhost:3001>
- **CrashSafe dashboard**: <http://localhost:3000>

## Recommended verification workflow

1. **Open the test harness** and click **"Setup Demo Data"**. This creates:
   - `owdata/config` (5 docs) — always backed up regardless of prefix
   - `owdata/sensors---{temp-1, temp-2, humidity-1, pressure-1}` (100 docs each)
   - `owdata/not_a_sensor` (1 doc) — should appear in the harness with the **excluded** pill, *not* in any backup
   - `parse/items` (100 docs)
2. **Trigger a Full Backup** in the CrashSafe dashboard (per DB or via cron — the schedule in this compose is every 2 min).
3. Verify the backup landed under `local-test/backups/data/<id>/` and `local-test/backups/parse/<id>/`. The `not_a_sensor` collection should **not** be in the data dump.
4. **Modify and delete data** via the test harness — use the bulk buttons (`+50`, `~20`, `−10`) to simulate workload across all backed-up collections quickly.
5. **Trigger an Incremental Backup**. Inspect the new entry in the dashboard's history table — note its size relative to the full.
6. **Wipe All Databases** in the test harness (red button). Confirm both DBs are empty.
7. **In the CrashSafe dashboard**, on each DB card, click **"Wipe & Restore"**.
8. Verify counts in the test harness match what you had pre-wipe (modulo any ongoing changes during the wipe — but for a static state, they should match exactly).

## Variants worth testing

- **Restore from a point**: do a Full → make changes → Inc1 → make more changes → Inc2. Then "Wipe All", and use **"Restore from Point"** in the dashboard, picking a Full or Inc entry. Verify the data state matches what was on disk *at that point*.
- **Concurrent writes during backup**: open a busy bulk-insert in the harness while a backup is running. The test harness's progress + the dashboard's in-flight banner should both update; the backup should finish without errors.
- **Daemon restart**: `docker compose restart crashsafe`. The dashboard's "Last Run" cards should repopulate from the manifest after the restart (this verifies the startup-task seeding).
- **Auth**: uncomment the `OPENINC_MONGO_BACKUP_AUTH_*` lines in `docker-compose.yml`, run `docker compose up -d`. The dashboard now requires `admin` / `test123`.

## Inspecting state directly

```bash
# Tail the daemon logs
docker compose logs -f crashsafe

# Inspect the lock file while a backup runs
docker compose exec crashsafe cat /backups/.backup.lock

# Look at the manifest for the data DB
docker compose exec crashsafe cat /backups/data/manifest.json | head -50

# Count docs in a collection from outside the harness
docker compose exec mongodb mongosh -u admin -p password --quiet --eval \
  'db.getSiblingDB("owdata").getCollection("sensors---temp-1").countDocuments()'
```

## Reset to a clean slate

```bash
docker compose down -v       # also drops the mongo-data volume
rm -rf backups               # wipes the bind-mounted backup dir
```

## Stress test: many collections + low RAM

Reproduces the failure mode where the daemon froze a production host with
thousands of sensor collections. The bug is non-obvious on the default
4-collection demo — it only shows up when WiredTiger's cache is small
relative to the working set and the daemon does its own read passes on top.

Three pieces are involved:

- `docker-compose.low-mem.yml` — caps `mongodb` at 256 MiB RAM (so WT
  evicts aggressively) and `crashsafe` at 128 MiB (so a runaway backup
  OOMs cleanly instead of dragging the host down).
- `docker-compose.low-prio.yml` — sets
  `OPENINC_MONGO_BACKUP_COLLECTION_PAUSE_MS=3000` and
  `OPENINC_MONGO_BACKUP_NICE_BACKUP=true`. This is the "polite backup"
  config you'd run on a single-mongod production server.
- `scripts/seed-many-collections.sh` — bulk-creates many
  `sensors---bulk-NNNNN` collections in `owdata` so the load profile
  matches a real deployment. Defaults to 25 000; override with
  `NUM=…`. Each collection gets a few docs with `updatedAt`, so
  incrementals see real work.

### Measurement: what gets captured

`run-backup-test.sh` does the whole observation cycle for you:

- samples `docker stats` every 2 s into a CSV (per container CPU /
  memory used / memory %),
- subscribes to `docker events` for the run window and logs every
  kernel OOM-kill,
- triggers the backup via the dashboard API and polls until the
  lockfile is released,
- on completion writes `summary.txt` with: duration, peak memory per
  container (absolute + % of cap), OOM count, container restart
  count, and slow-query count from the mongo log within the run window.

Run it once per scenario; artifacts land under `./runs/<name>/`.
Then diff the two summaries with `compare-runs.sh`.

### Seed first (without the RAM cap)

Creating thousands of fresh collections needs more memory than the
steady-state workload — WiredTiger has to allocate catalog entries +
connection-pool buffers on every new namespace. Under the low-mem
override the seed OOM-kills Mongo. So seed against the *unconstrained*
stack first, then apply the cap for the actual backup test:

```bash
cd local-test

# Plain stack — no RAM cap.
docker compose up -d --build
docker compose ps   # wait until mongodb is "healthy"

# Seed. Start small to confirm the pipeline works, then scale up.
# 5 000 finishes in a couple of minutes on a typical laptop;
# 25 000 can take 10-15 minutes and ~2 GiB of disk under ./backups + the mongo volume.
NUM=5000 ./scripts/seed-many-collections.sh

# Verify the count landed
docker compose exec -T mongodb mongosh -u admin -p password \
    --authenticationDatabase admin --quiet \
    --eval 'print(db.getSiblingDB("owdata").getCollectionNames().length)'
# Expect: 5000

# Stop without -v to keep the Mongo volume (seeded data stays on disk)
docker compose down
```

### Run A: baseline (old behaviour, low RAM)

```bash
# Bring the stack back up WITH the RAM cap. Mongo finds the seeded data
# already on the volume — no reseeding needed.
docker compose \
    -f docker-compose.yml \
    -f docker-compose.low-mem.yml \
    up -d --build

# Wait for healthy (a bit slower under the cap)
docker compose ps

# Measured run. Default target=data, type=full.
./scripts/run-backup-test.sh baseline
```

While it runs, the script prints elapsed time + the collection
currently being processed (read from `/api/status` → `inFlight`).
When the lockfile is released, it tears down the samplers and prints
the summary.

What you should see in `runs/baseline/summary.txt`:

- mongo peak mem ~100 % of the 256 MiB cap.
- `OOM events during the run: count: N>0` if the daemon got
  killed — usually visible at higher collection counts (10 k+).
- Many slow queries in mongo log.

### Run B: polite backup (new settings, same low RAM)

Keep the seeded data — no need to wipe between runs. Same low-RAM cap,
but layer the polite-backup config on top:

```bash
docker compose \
    -f docker-compose.yml \
    -f docker-compose.low-mem.yml \
    -f docker-compose.low-prio.yml \
    up -d --build

# Confirm the env vars landed:
docker compose exec crashsafe printenv | grep -E '(PAUSE_MS|NICE_BACKUP)'
# Expect:
#   OPENINC_MONGO_BACKUP_COLLECTION_PAUSE_MS=3000
#   OPENINC_MONGO_BACKUP_NICE_BACKUP=true

# Spot-check that mongodump actually runs at idle priority — `NI` column
# should read 19 once a backup is in progress.
docker compose exec crashsafe ps -o pid,ni,cmd ax | grep mongodump

# Measured run.
./scripts/run-backup-test.sh lowprio
```

While the polite run is happening, click `+50 in all` in
<http://localhost:3001> — those inserts should still go through with
normal latency. In Run A they typically hang or time out.

### Comparing the two runs

```bash
./scripts/compare-runs.sh baseline lowprio
```

On a wide terminal this prints both `summary.txt` files side by side
(`diff -y`). The signals to look for:

| Signal | Run A (baseline) | Run B (low-prio) |
|---|---|---|
| mongo peak mem % | ~100 % | 60-95 %, oscillating |
| crashsafe OOMKilled (state.OOMKilled=true) | likely on 25 000 colls | should not happen |
| Slow-query count | hundreds-thousands | tens or fewer |
| testapp inserts during backup | hang / time out | normal latency |
| Total backup duration | shorter (until it crashes) | longer (but completes) |
| `ps` shows `mongodump` nice value | 0 | 19 |

If Run B still struggles, raise `COLLECTION_PAUSE_MS` to 5000 or
10000 in `docker-compose.low-prio.yml` and run again. If it's still
not enough, the next lever is to enable
`OPENINC_MONGO_BACKUP_APPEND_ONLY_DATA=true` (already the default in
the main compose) — but be aware that loses delete detection.

### Raw artifacts per run

```
runs/<name>/
  stats.csv        # timestamp,container,cpu%,memUsed,mem%
  oom.log          # kernel OOM events (one per line)
  summary.txt      # human-readable report
```

You can graph `stats.csv` in anything that reads CSV (Excel,
`gnuplot`, a Jupyter notebook) — the columns are stable.

### Reset between runs

```bash
docker compose \
    -f docker-compose.yml \
    -f docker-compose.low-mem.yml \
    -f docker-compose.low-prio.yml \
    down -v
rm -rf backups
```

## Troubleshooting

- **`crashsafe` keeps restarting**: check `docker compose logs crashsafe` — usually an env-var typo or Mongo not yet healthy. The compose's `depends_on: condition: service_healthy` should handle the latter.
- **`testapp` shows "Disconnected"**: Mongo is still starting up; refresh in 5–10s.
- **Want a slower cron**: change `OPENINC_MONGO_BACKUP_CRON` in `docker-compose.yml` and `docker compose up -d` to recreate the container.
