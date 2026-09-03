#!/usr/bin/env bash
# Figure 1: the good-neighbour claim in REAL UNITS.
#
# Samples the NC JVMs' CPU (% of one core) and block-device I/O (MB/s) at 10Hz while a single sort
# runs, for stock vs bucketed. Replaces `spreadPct`, which is a unit only we understand.
#
# Expected shape: stock does its I/O in a block that falls to zero, then spikes CPU; bucketed
# sustains both at moderate levels because sort work is interleaved with arrival.
#
# Run UNDER THE BALLOON: cache-resident, `read_bytes` counts only real device reads, so both arms
# would show ~no I/O and the figure would be an artifact rather than a finding.
set -uo pipefail
cd "$(dirname "$0")"
EXP=$(cd ../.. && pwd)
export REPO_ROOT=~/Ameen/asterixdb JARDIR=~/Ameen/jars
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
CL=$REPO_ROOT/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT/opt/local
Q=http://127.0.0.1:19002/query/service
OUT=~/Ameen/trace; mkdir -p $OUT
DV=${DV:-tpcds}; TB=${TB:-store_sales}; COL=${COL:-ss_sales_price}; MEM=${MEM:-64MB}
BALLOON_GB=${BALLOON_GB:-44}
echo RUNNING > ~/Ameen/trace.status
beat(){ echo "$(date +%H:%M:%S) $1" > ~/Ameen/trace.heartbeat; }
cleanup(){ [ -n "${SPID:-}" ] && kill "$SPID" 2>/dev/null; [ -n "${BPID:-}" ] && kill "$BPID" 2>/dev/null; }
trap cleanup EXIT INT TERM
fail(){ echo "FATAL: $1" >&2; echo FAILED > ~/Ameen/trace.status; exit 1; }
dep(){ local tag=$1; shift
  (cd $EXP && ./deploy.sh "$@") > $OUT/deploy-$tag.log 2>&1 || fail "DEPLOY FAILED $tag"
  grep -q 'jar verified' $OUT/deploy-$tag.log || fail "jar not verified $tag"; }
alive(){ [ "$(curl -s -m 25 -o /dev/null -w '%{http_code}' "$Q" --data-urlencode 'statement=SELECT 1;')" = 200 ]; }
qt(){ curl -s -o /dev/null -m 7200 -w '%{time_total}' "$Q" --data-urlencode \
  "statement=USE $DV; SET \`compiler.sortmemory\` \"$MEM\"; SELECT VALUE count(*) FROM (SELECT VALUE r FROM $TB AS r ORDER BY r.$COL) AS x;"; }

~/Ameen/balloon "$BALLOON_GB" > $OUT/balloon.log 2>&1 & BPID=$!
sleep 45; grep -q holding $OUT/balloon.log || fail "balloon did not start"
echo "cache under balloon: $(free -g | awk '/^Mem:/{print $6}')GB" | tee $OUT/mem.txt

: > $OUT/summary.txt
for ARM in stock bucket; do
  beat "$ARM deploy"
  if [ $ARM = stock ]; then dep "$ARM" --jar $JARDIR/master.jar --broker stock --heap 6g
  else dep "$ARM" --jar $JARDIR/adaptive.jar --broker none --heap 6g --cap-mult 1 \
           --auto-type-key true --kway true --merge-fan-in 1000000; fi
  alive || fail "cluster down after deploy $ARM"
  (cd $EXP && ./drop_caches.sh "$CL/data") >> $OUT/drop.log 2>&1 || true
  beat "$ARM warmup"; qt > /dev/null            # JIT/cluster warm-up, not traced
  (cd $EXP && ./drop_caches.sh "$CL/data") >> $OUT/drop.log 2>&1 || true
  beat "$ARM trace"
  python3 sidecar.py "$OUT/$ARM.csv" 0.1 asterixnc & SPID=$!
  sleep 1
  T=$(qt)
  sleep 1; kill "$SPID" 2>/dev/null; wait "$SPID" 2>/dev/null || true; SPID=""
  # Sanity: the traced bytes must be in the right ballpark for the dataset, or the counters are
  # measuring page-cache hits (which read_bytes should exclude) rather than device traffic.
  TOT=$(awk -F, 'NR>1{r+=$3*0.1; w+=$4*0.1} END{printf "read=%.1fGB write=%.1fGB", r/1024, w/1024}' "$OUT/$ARM.csv")
  echo "$ARM time=${T}s samples=$(($(wc -l < $OUT/$ARM.csv)-1)) $TOT" | tee -a $OUT/summary.txt
done
echo DONE > ~/Ameen/trace.status; beat done
