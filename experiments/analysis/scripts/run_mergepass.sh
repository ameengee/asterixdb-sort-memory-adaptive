#!/usr/bin/env bash
# Does more memory help once merge PASSES are actually at stake?
#
# Total comparisons are ~N*log2(runSize) + N*log2(numRuns), and those sum to N*log2(N) -- a constant.
# Shifting memory only MOVES work between the run phase and the merge phase; it never reduces it.
# The one thing that genuinely reduces work is needing FEWER MERGE PASSES.
#
# Every 7GB measurement so far used >=32MB, where the frame budget affords a fan-in around 1023 and
# ~294 runs merge in a SINGLE pass. More memory therefore bought nothing while costing extra
# comparisons (measured: 15.8 -> 20.8 per tuple from 8MB to 2GB). BELOW 32MB the available fan-in
# shrinks while the run count grows, so the merge needs multiple passes -- and that is where extra
# memory should pay for itself.
#
# Prediction: steep improvement from 4MB to ~32MB (passes 2+ -> 1), then flat or slightly worse.
set -uo pipefail
cd "$(dirname "$0")"
EXP=$(cd ../.. && pwd)
export REPO_ROOT=~/Ameen/asterixdb JARDIR=~/Ameen/jars
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
CL=$REPO_ROOT/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT/opt/local
Q=http://127.0.0.1:19002/query/service
OUT=~/Ameen/mergepass; mkdir -p $OUT
MEMS=${MEMS:-"8MB 16MB 32MB 64MB 128MB 512MB"}
ROUNDS=${ROUNDS:-1}; REPS=${REPS:-2}
echo RUNNING > ~/Ameen/mergepass.status
beat(){ echo "$(date +%H:%M:%S) $1" > ~/Ameen/mergepass.heartbeat; }
fail(){ echo "$1" >&2; echo FAILED > ~/Ameen/mergepass.status; exit 1; }
dep(){ local tag=$1; shift
  (cd $EXP && ./deploy.sh "$@") > $OUT/deploy-$tag.log 2>&1 || fail "DEPLOY FAILED $tag"
  grep -q 'jar verified' $OUT/deploy-$tag.log || fail "jar not verified $tag"; }
snap(){ : > $OUT/.snap; for f in $CL/logs/nc-*.log; do [ -f "$f" ] && echo "$f $(wc -l < "$f")" >> $OUT/.snap; done; }
since(){ while read -r f n; do tail -n +$((n+1)) "$f"; done < $OUT/.snap; }
qt(){ curl -s -o /dev/null -m 7200 -w '%{time_total}' "$Q" --data-urlencode \
  "statement=USE $1; SET \`compiler.sortmemory\` \"$2\"; SELECT VALUE count(*) FROM (SELECT VALUE r FROM ds AS r ORDER BY r.k) AS x;"; }

: > $OUT/times.txt; : > $OUT/runs.txt
for ROUND in $(seq 1 $ROUNDS); do
  for ARM in ours stock-typed; do
    beat "round$ROUND $ARM deploy"
    if [ $ARM = ours ]; then
      dep "$ARM-r$ROUND" --jar $JARDIR/adaptive.jar --broker none --heap 6g --cap-mult 1 \
          --auto-type-key true --kway true --merge-fan-in 1000000; DV=big
    else
      dep "$ARM-r$ROUND" --jar $JARDIR/master.jar --broker stock --heap 6g; DV=bigtyped
    fi
    for MEM in $MEMS; do
      beat "round$ROUND $ARM $MEM"; snap
      # A sort of 290M rows cannot finish in under a second. A "time" that small means the
      # request never ran (connection refused after a cluster death), and recording it would
      # silently poison the series -- which is exactly what happened at 4MB.
      alive(){ [ "$(curl -s -m 20 -o /dev/null -w '%{http_code}' "$Q" \
                    --data-urlencode 'statement=SELECT 1;')" = 200 ]; }
      alive || fail "cluster unreachable before $ARM @ $MEM"
      qt $DV $MEM >/dev/null   # warm-up, discarded
      for r in $(seq 1 $REPS); do
        T=$(qt $DV $MEM)
        if awk -v t="$T" 'BEGIN{exit !(t < 1.0)}'; then
          alive || fail "cluster died during $ARM @ $MEM (query returned ${T}s)"
          fail "implausible ${T}s for a 290M-row sort at $ARM @ $MEM -- refusing to record it"
        fi
        echo "MP $ROUND $ARM $MEM $T" >> $OUT/times.txt
      done
      since | grep -c 'adaptive-sort-run:' | sed "s/^/RUNS $ARM $MEM /" >> $OUT/runs.txt
      since | grep -o 'adaptive-sort-run:.*tuples=[0-9]*' | head -1 | sed "s/^/RUN1 $ARM $MEM /" >> $OUT/runs.txt
    done
  done
done
echo DONE > ~/Ameen/mergepass.status; beat done
