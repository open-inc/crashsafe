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

## Troubleshooting

- **`crashsafe` keeps restarting**: check `docker compose logs crashsafe` — usually an env-var typo or Mongo not yet healthy. The compose's `depends_on: condition: service_healthy` should handle the latter.
- **`testapp` shows "Disconnected"**: Mongo is still starting up; refresh in 5–10s.
- **Want a slower cron**: change `OPENINC_MONGO_BACKUP_CRON` in `docker-compose.yml` and `docker compose up -d` to recreate the container.
