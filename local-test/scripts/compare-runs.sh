#!/usr/bin/env bash
# Side-by-side diff of two measured runs. Prints the two summary.txt files
# next to each other so peaks / durations / OOM counts are easy to compare.
#
# Usage:
#   ./scripts/compare-runs.sh baseline lowprio

set -euo pipefail

A="${1:-}"
B="${2:-}"
if [[ -z "$A" || -z "$B" ]]; then
    echo "Usage: $0 <run-a> <run-b>" >&2
    exit 1
fi

cd "$(dirname "$0")/.."

SA="./runs/$A/summary.txt"
SB="./runs/$B/summary.txt"
if [[ ! -f "$SA" || ! -f "$SB" ]]; then
    echo "Missing summary file(s). Make sure both runs completed:" >&2
    [[ ! -f "$SA" ]] && echo "  $SA"
    [[ ! -f "$SB" ]] && echo "  $SB"
    exit 1
fi

# `diff -y` (side-by-side) on a wide terminal renders nicely. Fall back to
# sequential print on narrow terminals to avoid mangled output.
cols=$(tput cols 2>/dev/null || echo 80)
if [[ "$cols" -ge 140 ]]; then
    diff --side-by-side --width="$cols" "$SA" "$SB" || true
else
    echo "=== $A ==="
    cat "$SA"
    echo
    echo "=== $B ==="
    cat "$SB"
fi
