#!/usr/bin/env bash
# WHERE does the 29-40% reclamation cost actually go, and WHICH memory do we give up?
#
# Two questions, two instruments:
#   1. Tier mix -- every reclaim decision now logs easy/medium/hard frames. shrinkTo can only
#      release FREE frames (easy); medium and hard become easy only by SPILLING first. So the mix
#      tells us whether a reclaim was free (easy available) or forced work (medium/hard spilled).
#   2. Time attribution -- the phase counters split a run into load / sort / cascade / merge /
#      flush. If forced spills produce more, smaller runs, the cost should appear in flush+merge
#      and in the RUN COUNT, not in sorting.
set -uo pipefail
cd "$(dirname "$0")"
EXP=$(cd ../.. && pwd)
export REPO_ROOT=~/Ameen/asterixdb JARDIR=~/Ameen/jars
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
CL=$REPO_ROOT/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT/opt/local
Q=http://127.0.0.1:19002/query/service
OUT=~/Ameen/anatomy; mkdir -p $OUT
DV=${DV:-big}; MEMS=${MEMS:-"512MB"}; REPS=${REPS:-3}
COMMON="--heap 6g --cap-mult 1 --auto-type-key true --kway true --merge-fan-in 1000000 --phase-log true"
RECLAIM="--broker periodic --period 10 --action reclaim --fraction 0.5"

echo RUNNING > ~/Ameen/anatomy.status
beat(){ echo "$(date +%H:%M:%S) $1" > ~/Ameen/anatomy.heartbeat; }
fail(){ echo "$1" >&2; echo FAILED > ~/Ameen/anatomy.status; exit 1; }
dep(){ local tag=$1; shift
  (cd $EXP && ./deploy.sh "$@") > $OUT/deploy-$tag.log 2>&1 || fail "DEPLOY FAILED $tag"
  grep -q 'jar verified' $OUT/deploy-$tag.log || fail "jar not verified $tag"; }
snap(){ : > $OUT/.snap; for f in $CL/logs/nc-*.log; do [ -f "$f" ] && echo "$f $(wc -l < "$f")" >> $OUT/.snap; done; }
since(){ while read -r f n; do tail -n +$((n+1)) "$f"; done < $OUT/.snap; }
qt(){ curl -s -o /dev/null -m 7200 -w '%{time_total}' "$Q" --data-urlencode \
  "statement=USE $1; SET \`compiler.sortmemory\` \"$2\"; SELECT VALUE count(*) FROM (SELECT VALUE r FROM ds AS r ORDER BY r.k) AS x;"; }

: > $OUT/times.txt; : > $OUT/phases.txt; : > $OUT/tiers.txt
for ARM in no-pressure reclaim; do
  beat "$ARM deploy"
  if [ $ARM = no-pressure ]; then dep "$ARM" --jar $JARDIR/adaptive.jar --broker none $COMMON
  else dep "$ARM" --jar $JARDIR/adaptive.jar $RECLAIM $COMMON --release-on-shrink true; fi
  for MEM in $MEMS; do
    beat "$ARM $MEM"; snap
    for r in $(seq 1 $REPS); do echo "AN $ARM $MEM $(qt $DV $MEM)" >> $OUT/times.txt; done
    since | grep -o 'adaptive-sort-phase:.*' | sed "s/^/PH $ARM $MEM /" >> $OUT/phases.txt
    # tier mix at each reclaim decision, and how much memory was actually handed back
    since | grep -oE 'adaptive-sort: [a-z-]+ .*easy=[0-9]+ medium=[0-9]+ hard=[0-9]+' \
      | sed "s/^/TIER $ARM $MEM /" >> $OUT/tiers.txt
    since | grep -o 'released [0-9]* bytes back to the frame manager' \
      | sed "s/^/REL $ARM $MEM /" >> $OUT/tiers.txt
  done
done
echo DONE > ~/Ameen/anatomy.status; beat done
