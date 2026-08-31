#!/usr/bin/env bash
# E1 -- "no harm": stock master vs adaptive-with-inert-broker.
#
# Blocks ALTERNATE (stock, adaptive, stock, adaptive, ...) rather than running all of one arm
# then all of the other. Machine state drifts -- thermal, page cache, background load -- and a
# blocked design would attribute that drift to the treatment. Alternating lets us test for it.
set -euo pipefail
cd "$(dirname "$0")"

ROUNDS="${ROUNDS:-3}"
BLOCK="${BLOCK:-180}"        # seconds per block
WARMUP="${WARMUP:-40}"       # queries discarded per block (see note below)
SORTMEM="${SORTMEM:-8MB}"
REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
CLUSTER="${CLUSTER:-$REPO_ROOT/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT/opt/local}"
JARDIR="${JARDIR:-/tmp/jars}"

run_block () {
  local arm="$1" jar="$2" broker="$3" round="$4"
  local label="e1-${arm}-r${round}"
  echo "=== [$(date +%H:%M:%S)] block: $label ==="
  ./deploy.sh --jar "$jar" --broker "$broker" >/dev/null 2>&1 || {
      echo "deploy failed for $label" >&2; exit 1; }
  # confirm what actually loaded (empty for stock, which has no broker line)
  ./scrape_sort_logs.py --logs "$CLUSTER/logs" --label _reset --outdir /tmp/scratch \
      --since-line-file "/tmp/e1-${arm}.since" >/dev/null 2>&1
  ./run_workload.py --label "$label" --duration "$BLOCK" --sort-memory "$SORTMEM" --warmup "$WARMUP" \
      >/dev/null 2>&1
  ./scrape_sort_logs.py --logs "$CLUSTER/logs" --label "$label" \
      --since-line-file "/tmp/e1-${arm}.since" 2>/dev/null | sed 's/^/    /'
}

for r in $(seq 1 "$ROUNDS"); do
  run_block stock    "$JARDIR/master.jar"   stock r$r
  run_block adaptive "$JARDIR/adaptive.jar" none  r$r
done
echo "=== E1 complete ==="
