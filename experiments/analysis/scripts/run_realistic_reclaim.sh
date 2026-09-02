#!/usr/bin/env bash
# What does a REALISTIC reclaim cost?
#
# The earlier figure (+29-40%) came from the periodic broker, which takes half the remaining budget
# every 10 polls and never stops -- the budget collapsed 2047 -> 511 -> ... -> 16 frames and the sort
# ran 7GB in 2MB. That measures "being squeezed to nothing", not "surrendering memory politely".
#
# These arms use the SCRIPTED broker for bounded demands, which is what a neighbouring query
# actually looks like. Also records the easy/medium/hard tier mix at each decision, to show whether
# the reclaim took the free path (lower the budget, spill nothing) or was forced to spill.
set -uo pipefail
cd "$(dirname "$0")"
EXP=$(cd ../.. && pwd)
export REPO_ROOT=~/Ameen/asterixdb JARDIR=~/Ameen/jars
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
CL=$REPO_ROOT/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT/opt/local
Q=http://127.0.0.1:19002/query/service
OUT=~/Ameen/realistic; mkdir -p $OUT
DV=${DV:-big}; MEMS=${MEMS:-"512MB"}; ROUNDS=${ROUNDS:-2}; REPS=${REPS:-2}
SCRIPTS=$(cd brokers && pwd)
COMMON="--heap 6g --cap-mult 1 --auto-type-key true --kway true --merge-fan-in 1000000 --phase-log true"

echo RUNNING > ~/Ameen/realistic.status
beat(){ echo "$(date +%H:%M:%S) $1" > ~/Ameen/realistic.heartbeat; }
fail(){ echo "$1" >&2; echo FAILED > ~/Ameen/realistic.status; exit 1; }
dep(){ local tag=$1; shift
  (cd $EXP && ./deploy.sh "$@") > $OUT/deploy-$tag.log 2>&1 || fail "DEPLOY FAILED $tag"
  grep -q 'jar verified' $OUT/deploy-$tag.log || fail "jar not verified $tag"; }
snap(){ : > $OUT/.snap; for f in $CL/logs/nc-*.log; do [ -f "$f" ] && echo "$f $(wc -l < "$f")" >> $OUT/.snap; done; }
since(){ while read -r f n; do tail -n +$((n+1)) "$f"; done < $OUT/.snap; }
qt(){ curl -s -o /dev/null -m 7200 -w '%{time_total}' "$Q" --data-urlencode \
  "statement=USE $1; SET \`compiler.sortmemory\` \"$2\"; SELECT VALUE count(*) FROM (SELECT VALUE r FROM ds AS r ORDER BY r.k) AS x;"; }

: > $OUT/times.txt; : > $OUT/tiers.txt; : > $OUT/budgets.txt
for ROUND in $(seq 1 $ROUNDS); do
  for ARM in none light moderate repeated periodic-old; do
    beat "round$ROUND $ARM"
    case $ARM in
      none)         dep "$ARM-r$ROUND" --jar $JARDIR/adaptive.jar --broker none $COMMON ;;
      periodic-old) dep "$ARM-r$ROUND" --jar $JARDIR/adaptive.jar --broker periodic --period 10 \
                        --action reclaim --fraction 0.5 $COMMON --release-on-shrink true ;;
      *)            dep "$ARM-r$ROUND" --jar $JARDIR/adaptive.jar --broker scripted \
                        --script $SCRIPTS/$ARM.csv $COMMON --release-on-shrink true ;;
    esac
    for MEM in $MEMS; do
      beat "round$ROUND $ARM $MEM"; snap
      for r in $(seq 1 $REPS); do echo "RR $ROUND $ARM $MEM $(qt $DV $MEM)" >> $OUT/times.txt; done
      # which path each decision took, and the tier mix at that moment
      since | grep -oE 'adaptive-sort: [a-z-]+ budgetFrames=[0-9]+ .*easy=[0-9]+ medium=[0-9]+ hard=[0-9]+' \
        | sed "s/^/TIER $ARM $MEM /" >> $OUT/tiers.txt
      since | grep -o 'adaptive-sort: [a-z-]* budgetFrames=[0-9]*' | awk '{print $2, $3}' \
        | sort | uniq -c | sed "s/^/BUD $ARM $MEM /" >> $OUT/budgets.txt
    done
  done
done
echo DONE > ~/Ameen/realistic.status; beat done
