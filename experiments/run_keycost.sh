#!/usr/bin/env bash
# WHERE does the auto-key's memory-dependent cost come from? Tie-breaking is ruled out
# (fallback rate ~0), so measure the sort phase directly instead of theorizing:
#   compares/tuple and ns/compare for the auto key vs the native decisive key,
#   at a small and a large budget. If ns/compare is flat and compares/tuple grows
#   identically for both arms, the cost is NOT in comparison at all and we look elsewhere.
set -uo pipefail
cd "$(dirname "$0")"
export REPO_ROOT=~/Ameen/asterixdb JARDIR=~/Ameen/jars
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
CL=$REPO_ROOT/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT/opt/local
Q=http://127.0.0.1:19002/query/service
OUT=~/Ameen/keycost; mkdir -p $OUT
REPS=${REPS:-4}; NOBUCKET=2000000000
echo RUNNING > ~/Ameen/keycost.status
beat(){ echo "$(date +%H:%M:%S) $1" > ~/Ameen/keycost.heartbeat; }
fail(){ echo "$1" >&2; echo FAILED > ~/Ameen/keycost.status; exit 1; }
dep(){ local tag=$1; shift
  ./deploy.sh "$@" > $OUT/deploy-$tag.log 2>&1 || fail "DEPLOY FAILED $tag"
  grep -q 'jar verified' $OUT/deploy-$tag.log || fail "jar not verified $tag"; }
# Per-file log offsets. Snapshotting the line count of `cat nc-*.log` and later tailing the
# re-concatenation is WRONG: once both files grow, the first N lines of the concatenation are
# no longer the old content, so earlier cells' lines leak into this cell's capture.
snap(){ : > $OUT/.logsnap; for f in $CL/logs/nc-*.log; do
          [ -f "$f" ] && echo "$f $(wc -l < "$f")" >> $OUT/.logsnap; done; }
since(){ while read -r f n; do tail -n +$((n+1)) "$f"; done < $OUT/.logsnap; }
qt(){ curl -s -o /dev/null -m 3600 -w '%{time_total}' "$Q" --data-urlencode \
  "statement=USE $1; SET \`compiler.sortmemory\` \"$2\"; SELECT VALUE count(*) FROM (SELECT VALUE r FROM ds AS r ORDER BY r.k) AS x;"; }
: > $OUT/times.txt; : > $OUT/phases.txt
C="--jar $JARDIR/adaptive.jar --broker none --heap 6g --kway true --merge-fan-in 1000000 \
   --cap-mult 1 --bucket-tuples $NOBUCKET --phase-log true"
for ROUND in 1 2 3; do
 for ARM in autokey neither; do
  beat "r$ROUND $ARM deploy"
  if [ $ARM = autokey ]; then dep "$ARM-r$ROUND" $C --auto-type-key true; DV=test
  else                        dep "$ARM-r$ROUND" $C;                       DV=typed; fi
  for MEM in 8MB 2048MB; do
    beat "r$ROUND $ARM $MEM"
    snap
    qt $DV $MEM >/dev/null
    for r in $(seq 1 $REPS); do echo "KC $ROUND $ARM $MEM $(qt $DV $MEM)" >> $OUT/times.txt; done
    since \
      | grep -o 'adaptive-sort-phase:.*' | sed "s/^/PH $ARM $MEM /" >> $OUT/phases.txt
  done
 done
done
echo DONE > ~/Ameen/keycost.status; beat done
