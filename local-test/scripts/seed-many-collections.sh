#!/usr/bin/env bash
# Bulk-create many sensor-style collections in the local-test Mongo so the
# load profile matches a real "lots of collections" deployment.
#
# Defaults: 25 000 collections in the `owdata` DB, 5 docs each, named
# `sensors---bulk-NNNNN`. Override via env vars:
#
#   NUM=5000 DOCS_PER=10 ./seed-many-collections.sh
#
# Run from the local-test/ directory (where docker-compose.yml lives) with
# the stack already up. The script reuses the running `mongodb` service via
# `docker compose exec` — no extra tooling on the host needed.

set -euo pipefail

NUM=${NUM:-25000}
DOCS_PER=${DOCS_PER:-5}
DB=${DB:-owdata}
PREFIX=${PREFIX:-sensors---bulk-}

cd "$(dirname "$0")/.."

if ! docker compose ps mongodb --status running --quiet | grep -q .; then
    echo "Error: mongodb service is not running. Start the stack first:" >&2
    echo "  docker compose up -d" >&2
    exit 1
fi

echo "Seeding $NUM collections into $DB ($DOCS_PER docs each, prefix '$PREFIX')..."
echo "(Run on a fresh stack; this only inserts, it does not clean up first.)"
echo

START=$(date +%s)

# Write the JS to a tempfile and copy it into the container, then run via
# `mongosh --file`. Earlier attempts using heredoc-as-stdin and
# `read -r -d ''` both proved flaky — mongosh's REPL-mode handling of
# piped stdin is line-buffered (multi-line constructs fail silently), and
# `read -d ''` truncated the script in subtle ways. `--file` reads the
# whole script as one parsed unit, deterministically. Tempfile is cleaned
# up on exit regardless of outcome.

TMP_SCRIPT=$(mktemp -t crashsafe-seed.XXXXXX)
trap 'rm -f "$TMP_SCRIPT"' EXIT

cat > "$TMP_SCRIPT" <<EOF
const db = db.getSiblingDB("$DB");
const num = $NUM;
const docsPer = $DOCS_PER;
const prefix = "$PREFIX";
const reportEvery = 1000;
const t0 = Date.now();
for (let i = 0; i < num; i++) {
    const name = prefix + String(i).padStart(5, "0");
    const docs = [];
    for (let j = 0; j < docsPer; j++) {
        docs.push({ ts: new Date(), val: Math.random(), updatedAt: new Date() });
    }
    db.getCollection(name).insertMany(docs);
    if ((i + 1) % reportEvery === 0) {
        const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
        print("  " + (i + 1) + "/" + num + " collections (" + elapsed + "s)");
    }
}
print("done.");
EOF

docker compose cp "$TMP_SCRIPT" mongodb:/tmp/seed.js

# Drop --quiet so any mongosh error surfaces. Auth + connection ping is
# still suppressed quickly; the only extra output is the welcome banner,
# which is fine — it confirms mongosh actually started.
docker compose exec -T mongodb mongosh \
    -u admin -p password --authenticationDatabase admin \
    --file /tmp/seed.js

END=$(date +%s)
echo
echo "Seeding finished in $((END - START)) seconds."
echo "Now trigger a backup from the dashboard at http://localhost:3000"
