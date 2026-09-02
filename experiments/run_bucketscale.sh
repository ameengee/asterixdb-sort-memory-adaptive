#!/usr/bin/env bash
# If bucketing is what makes us slower as memory grows, the likely reason is that the bucket
# target is a CONSTANT while the run size grows with memory -- so buckets/run climbs and the
# cascade merge does more and more work per tuple.
#
# This sweeps bucket size AT each memory level. What we want to see: the best bucket size
# grows with memory, and tracking it flattens our curve. If the best bucket size is constant
# and the curve still rises, the cost is in the cascade itself, not in bucket sizing.
set -uo pipefail
cd "$(dirname "$0")"
export REPO_ROOT=~/Ameen/asterixdb JARDIR=~/Ameen/jars
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
CL=$REPO_ROOT/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT/opt/local
Q=http://127.0.0.1:19002/query/service
OUT=~/Ameen/bucketscale; mkdir -p $OUT
ROUNDS=${ROUNDS:-3}; REPS=${REPS:-3}
MEMS=${MEMS:-"32MB 320MB 2048MB"}
BUCKETS=${BUCKETS:-"16384 65536 262144 1048576 2000000000"}   # last == bucketing off

echo RUNNING > ~/Ameen/bucketscale.status
beat(){ echo "$(date +%H:%M:%S) $1" > ~/Ameen/bucketscale.heartbeat; }
fail(){ echo "$1" >&2; echo FAILED > ~/Ameen/bucketscale.status; exit 1; }
dep(){ local tag=$1; shift
  ./deploy.sh "$@" > $OUT/deploy-$tag.log 2>&1 || fail "DEPLOY FAILED $tag"
  grep -q 'jar verified' $OUT/deploy-$tag.log || fail "jar not verified $tag"; }
qt(){ curl -s -o /dev/null -m 3600 -w '%{time_total}' "$Q" --data-urlencode \
  "statement=USE $1; SET \`compiler.sortmemory\` \"$2\"; SELECT VALUE count(*) FROM (SELECT VALUE r FROM ds AS r ORDER BY r.k) AS x;"; }

: > $OUT/times.txt; : > $OUT/fanin.txt
for ROUND in $(seq 1 $ROUNDS); do
  for BT in $BUCKETS; do
    beat "round$ROUND bt=$BT deploy"
    dep "bt$BT-r$ROUND" --jar $JARDIR/adaptive.jar --broker none --heap 6g --kway true \
        --merge-fan-in 1000000 --cap-mult 1 --auto-type-key true --bucket-tuples $BT --phase-log true
    for MEM in $MEMS; do
      beat "round$ROUND bt=$BT $MEM"
      S=$(cat $CL/logs/nc-*.log 2>/dev/null | wc -l)
      qt test $MEM >/dev/null
      for r in $(seq 1 $REPS); do echo "BS $ROUND $BT $MEM $(qt test $MEM)" >> $OUT/times.txt; done
      cat $CL/logs/nc-*.log 2>/dev/null | tail -n +$((S+1)) \
        | grep -o 'adaptive-sort-phase:.*' | head -2 | sed "s/^/PH $BT $MEM /" >> $OUT/phases.txt
    done
  done
  echo "round $ROUND done: $(wc -l < $OUT/times.txt) samples" | tee -a $OUT/progress.txt
done

# ---- Phase 2: is it bucket SIZE or cascade WIDTH? -------------------------------------
# Same default 256KB buckets, but vary how many the cascade merges at once. If a narrow,
# multi-level cascade beats the single wide k-way merge at large budgets, the cost is the
# winner tree touching thousands of scattered regions -- not the bucket size at all.
for ROUND in $(seq 1 $ROUNDS); do
  for FI in 2 16 128 1000000; do
    beat "p2 round$ROUND fanin=$FI deploy"
    dep "fi$FI-r$ROUND" --jar $JARDIR/adaptive.jar --broker none --heap 6g --kway true \
        --merge-fan-in $FI --cap-mult 1 --auto-type-key true --phase-log true
    for MEM in 320MB 2048MB; do
      beat "p2 round$ROUND fanin=$FI $MEM"
      qt test $MEM >/dev/null
      for r in $(seq 1 $REPS); do echo "FI $ROUND $FI $MEM $(qt test $MEM)" >> $OUT/fanin.txt; done
    done
  done
  echo "phase2 round $ROUND done" | tee -a $OUT/progress.txt
done

echo DONE > ~/Ameen/bucketscale.status
beat done
