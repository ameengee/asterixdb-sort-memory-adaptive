#!/usr/bin/env bash
# What is the 128MB bump?
#
# Every configuration measured -- including untouched stock with a declared type -- is ~5-10% slower
# at 128MB than at either 32MB or 512MB. It is not our code. Three candidates, separated here:
#   GC        -- a heap/collector threshold crossed near that budget   -> compare GC pause totals
#   run count -- a merge-pass boundary (runs exceeding the fan-in)     -> compare runs and merge time
#   spill     -- the point where the sort starts spilling at all       -> compare frames/run and I/O
#
# A FINE sweep around the bump is what distinguishes them: a GC or spill threshold gives a sharp
# step, a merge-pass boundary gives a sawtooth that recurs at predictable multiples.
set -uo pipefail
cd "$(dirname "$0")"
EXP=$(cd ../.. && pwd)
export REPO_ROOT=~/Ameen/asterixdb JARDIR=~/Ameen/jars
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
CL=$REPO_ROOT/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT/opt/local
Q=http://127.0.0.1:19002/query/service
OUT=~/Ameen/bump; mkdir -p $OUT
MEMS=${MEMS:-"48MB 64MB 96MB 112MB 128MB 144MB 160MB 192MB 256MB 384MB 512MB"}
ROUNDS=${ROUNDS:-3}; REPS=${REPS:-3}

echo RUNNING > ~/Ameen/bump.status
beat(){ echo "$(date +%H:%M:%S) $1" > ~/Ameen/bump.heartbeat; }
fail(){ echo "$1" >&2; echo FAILED > ~/Ameen/bump.status; exit 1; }
dep(){ local tag=$1; shift
  (cd $EXP && ./deploy.sh "$@") > $OUT/deploy-$tag.log 2>&1 || fail "DEPLOY FAILED $tag"
  grep -q 'jar verified' $OUT/deploy-$tag.log || fail "jar not verified $tag"; }
qt(){ curl -s -o /dev/null -m 3600 -w '%{time_total}' "$Q" --data-urlencode \
  "statement=USE $1; SET \`compiler.sortmemory\` \"$2\"; SELECT VALUE count(*) FROM (SELECT VALUE r FROM ds AS r ORDER BY r.k) AS x;"; }
snap(){ : > $OUT/.snap; for f in $CL/logs/nc-*.log; do [ -f "$f" ] && echo "$f $(wc -l < "$f")" >> $OUT/.snap; done
        GC0=$(wc -l < /tmp/gc-nc.log 2>/dev/null || echo 0); echo $GC0 > $OUT/.gcsnap; }
since(){ while read -r f n; do tail -n +$((n+1)) "$f"; done < $OUT/.snap; }
gcsince(){ tail -n +$(( $(cat $OUT/.gcsnap) + 1 )) /tmp/gc-nc.log 2>/dev/null; }

: > $OUT/times.txt; : > $OUT/phases.txt; : > $OUT/gc.txt
for ROUND in $(seq 1 $ROUNDS); do
  # `stock+declared` is the cleanest probe: no code of ours involved, yet it still bumps.
  for ARM in stock-typed ours; do
    beat "round$ROUND $ARM deploy"
    if [ $ARM = stock-typed ]; then
      dep "$ARM-r$ROUND" --jar $JARDIR/master.jar --broker stock --heap 6g --gc-log true; DV=typed
    else
      dep "$ARM-r$ROUND" --jar $JARDIR/adaptive.jar --broker none --heap 6g --cap-mult 1 \
          --auto-type-key true --kway true --merge-fan-in 1000000 --phase-log true --gc-log true; DV=test
    fi
    for MEM in $MEMS; do
      beat "round$ROUND $ARM $MEM"; snap
      qt $DV $MEM >/dev/null
      for r in $(seq 1 $REPS); do echo "BU $ROUND $ARM $MEM $(qt $DV $MEM)" >> $OUT/times.txt; done
      # GC pause total attributable to this cell, and the run structure it produced
      gcsince | grep -oE '[0-9.]+ms' | tr -d 'ms' \
        | awk -v a="$ARM" -v m="$MEM" '{s+=$1; n++} END{printf "GC %s %s pauses=%d totalMs=%.1f\n", a, m, n, s}' >> $OUT/gc.txt
      since | grep -o 'adaptive-sort-run:.*' | head -3 | sed "s/^/RUN $ARM $MEM /" >> $OUT/phases.txt
      since | grep -o 'adaptive-sort-phase:.*' | head -2 | sed "s/^/PH $ARM $MEM /" >> $OUT/phases.txt
    done
  done
  echo "round $ROUND: $(wc -l < $OUT/times.txt) samples" | tee -a $OUT/progress.txt
done
echo DONE > ~/Ameen/bump.status; beat done
