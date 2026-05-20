#!/usr/bin/env bash
# End-to-end measured backup run. Triggers a backup, samples docker stats while
# it runs, watches for OOM-kills, and writes a summary on completion. Save the
# output of two runs (baseline + low-prio) side by side to compare.
#
# Usage:
#   ./scripts/run-backup-test.sh <run-name>
#
# Env overrides:
#   TARGET=data|parse|all   (default: data)
#   TYPE=full|incremental   (default: full)
#   SAMPLE_INTERVAL=2       (seconds between docker stats samples)
#   POLL_INTERVAL=5         (seconds between /api/status polls)
#   TIMEOUT=3600            (give up after N seconds — safety net)
#
# Artifacts land under ./runs/<run-name>/:
#   stats.csv     — per-container CPU / MEM samples
#   oom.log       — kernel OOM events observed via `docker events`
#   summary.txt   — final report (duration, peaks, OOM count, slow-query count)

set -euo pipefail

RUN_NAME="${1:-}"
if [[ -z "$RUN_NAME" ]]; then
    echo "Usage: $0 <run-name>   (e.g. baseline, lowprio)" >&2
    exit 1
fi

TARGET="${TARGET:-data}"
TYPE="${TYPE:-full}"
SAMPLE_INTERVAL="${SAMPLE_INTERVAL:-2}"
POLL_INTERVAL="${POLL_INTERVAL:-5}"
TIMEOUT="${TIMEOUT:-3600}"

cd "$(dirname "$0")/.."

OUTDIR="./runs/$RUN_NAME"
mkdir -p "$OUTDIR"

# Fail loud and early if the stack isn't running — silent backups never finish
# is a much worse UX than "your compose isn't up".
if ! docker compose ps crashsafe --status running --quiet | grep -q .; then
    echo "Error: crashsafe service is not running. Start the stack first." >&2
    exit 1
fi

echo "Run: $RUN_NAME (target=$TARGET, type=$TYPE)"
echo "Artifacts: $OUTDIR"
echo

# ---------------------------------------------------------------------------
# Background samplers
# ---------------------------------------------------------------------------
# Both run in the foreground shell as background jobs so we can kill them
# cleanly when the backup completes (or when the user Ctrl+C's). `docker
# events` is a streaming command — we redirect to oom.log and kill at the
# end. `docker stats --no-stream` polled in a loop is more portable than
# the streaming version (and we get our own timestamp per row).

STATS_CSV="$OUTDIR/stats.csv"
OOM_LOG="$OUTDIR/oom.log"

echo "timestamp,container,cpu_pct,mem_used,mem_pct" > "$STATS_CSV"

(
    while true; do
        ts=$(date +%s)
        docker stats --no-stream \
            --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}}' \
            2>/dev/null \
            | sed "s/^/$ts,/" \
            >> "$STATS_CSV" || true
        sleep "$SAMPLE_INTERVAL"
    done
) &
SAMPLER_PID=$!

docker events --filter "event=oom" \
    --format '{{.Time}} {{.Actor.Attributes.name}}' \
    > "$OOM_LOG" &
OOM_PID=$!

# Always tear down background processes on exit, even on Ctrl+C / crash.
cleanup() {
    kill "$SAMPLER_PID" "$OOM_PID" 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Trigger backup + poll for completion
# ---------------------------------------------------------------------------

START_TS=$(date +%s)
START_ISO=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

echo "Triggering backup at $START_ISO..."
trigger_response=$(curl -sS -u admin:test123 \
    -H 'Content-Type: application/json' \
    -H 'Origin: http://localhost:3000' \
    -X POST \
    "http://localhost:3000/api/trigger/backup" \
    -d "{\"target\":\"$TARGET\",\"type\":\"$TYPE\"}")
echo "  API response: $trigger_response"
echo

# Give the daemon a moment to grab the lock before we start polling — between
# the 202 response and the lockfile being written there's a small window
# where inFlight is still null.
sleep 3

echo "Waiting for backup to complete (timeout=${TIMEOUT}s)..."
LOCK_SEEN=false
while true; do
    NOW=$(date +%s)
    ELAPSED=$((NOW - START_TS))
    if [[ $ELAPSED -gt $TIMEOUT ]]; then
        echo "  TIMEOUT after ${ELAPSED}s — giving up. The backup may still be running in the container."
        break
    fi

    status=$(curl -sS -u admin:test123 "http://localhost:3000/api/status" 2>/dev/null || echo '{}')

    # inFlight is null when no operation holds the lock. We require seeing
    # the lock at least once before treating null as "done", to avoid the
    # race where polling beats the lock acquisition.
    if echo "$status" | grep -q '"inFlight":null'; then
        if $LOCK_SEEN; then
            echo "  done (lock released after ${ELAPSED}s)"
            break
        fi
    else
        if ! $LOCK_SEEN; then
            echo "  lock acquired, backup running..."
            LOCK_SEEN=true
        fi
        progress=$(echo "$status" | grep -o '"currentCollection":"[^"]*"' | head -1)
        if [[ -n "$progress" ]]; then
            echo "  t=${ELAPSED}s  $progress"
        else
            echo "  t=${ELAPSED}s  (running)"
        fi
    fi

    sleep "$POLL_INTERVAL"
done

END_TS=$(date +%s)
DURATION=$((END_TS - START_TS))

# Stop samplers before summarising so the CSV doesn't grow during the report.
cleanup
trap - EXIT

# ---------------------------------------------------------------------------
# Summarise
# ---------------------------------------------------------------------------
# All math in awk so we don't need jq / python. MemUsage from docker stats
# is formatted like "12.5MiB / 128MiB" — we parse the left side and
# normalise to MiB so the peak across runs is comparable.

SUMMARY="$OUTDIR/summary.txt"

# Peak memory per container in MiB.
peak_mem_mib_per_container=$(
    awk -F, 'NR>1 {
        cont = $2
        # $4 is "used / limit", e.g. "12.5MiB / 128MiB" — keep only "used"
        split($4, p, "/")
        used = p[1]
        gsub(/^ +| +$/, "", used)
        unit = used; sub(/[0-9.]+/, "", unit)
        val  = used; sub(/[A-Za-z]+$/, "", val)
        val += 0
        if (unit == "KiB" || unit == "kB") mib = val / 1024
        else if (unit == "MiB" || unit == "MB") mib = val
        else if (unit == "GiB" || unit == "GB") mib = val * 1024
        else if (unit == "B")                   mib = val / 1024 / 1024
        else                                    mib = val
        if (mib > peak[cont]) peak[cont] = mib
    }
    END {
        for (c in peak) printf "  %-30s %.1f MiB\n", c, peak[c]
    }' "$STATS_CSV" | sort
)

# Peak mem_pct per container (already a percentage).
peak_pct_per_container=$(
    awk -F, 'NR>1 {
        cont = $2
        pct = $5; gsub("%", "", pct); pct += 0
        if (pct > peak[cont]) peak[cont] = pct
    }
    END {
        for (c in peak) printf "  %-30s %.1f%%\n", c, peak[c]
    }' "$STATS_CSV" | sort
)

oom_count=$(wc -l < "$OOM_LOG" | tr -d ' ')

# Slow-query count from mongo log within the run window. Mongo 7 logs JSON,
# so we match on the message string. --since requires Docker-style time;
# the start ISO works.
slow_count=$(docker compose logs mongodb --since "$START_ISO" 2>/dev/null \
    | grep -c '"msg":"Slow query"' || true)

# Container restart counts during the window (an OOM-kill bumps the count).
restart_info=$(
    for c in $(docker compose ps --format '{{.Name}}' 2>/dev/null); do
        rc=$(docker inspect --format '{{.RestartCount}}' "$c" 2>/dev/null || echo "?")
        oom=$(docker inspect --format '{{.State.OOMKilled}}' "$c" 2>/dev/null || echo "?")
        printf "  %-30s restarts=%s  last_state_oom=%s\n" "$c" "$rc" "$oom"
    done
)

# Backup entry that landed on disk (so we can confirm it actually completed).
last_entry=$(curl -sS -u admin:test123 "http://localhost:3000/api/status" 2>/dev/null \
    | grep -o '"lastBackup":"[^"]*"' | head -1 | sed 's/"lastBackup":"\(.*\)"/\1/')

{
    echo "=========================================================="
    echo "Run:           $RUN_NAME"
    echo "Target / Type: $TARGET / $TYPE"
    echo "Started:       $START_ISO"
    echo "Duration:      ${DURATION}s"
    echo "Last backup id seen on dashboard: ${last_entry:-(none)}"
    echo
    echo "--- Peak memory (used) ---"
    echo "$peak_mem_mib_per_container"
    echo
    echo "--- Peak memory (% of limit) ---"
    echo "$peak_pct_per_container"
    echo
    echo "--- OOM events during the run ---"
    echo "  count: $oom_count"
    if [[ "$oom_count" != "0" ]]; then
        sed 's/^/  /' "$OOM_LOG"
    fi
    echo
    echo "--- Container state ---"
    echo "$restart_info"
    echo
    echo "--- Slow queries in mongo log during run ---"
    echo "  count: $slow_count"
    echo "=========================================================="
} | tee "$SUMMARY"

echo
echo "Full data in: $OUTDIR/"
