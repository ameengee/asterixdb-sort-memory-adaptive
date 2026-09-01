#!/usr/bin/env bash
# Three-line figure: does our sorter do no harm, and does more memory make US faster?
#
#   stock        master jar, UNTYPED column (what a user gets by default)
#   stock-typed  master jar, TYPED column   (what a user gets if they hand-write the schema)
#   ours         adaptive jar, UNTYPED column + auto-detected key + bucketing + k-way
#
# Arms are interleaved inside each round so the cross-restart variance (~12%, measured)
# lands on every arm equally instead of biasing whichever arm we happened to deploy first.
set -uo pipefail
cd "$(dirname "$0")"
export REPO_ROOT=~/Ameen/asterixdb JARDIR=~/Ameen/jars
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
CL=$REPO_ROOT/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT/opt/local
Q=http://127.0.0.1:19002/query/service
OUT=~/Ameen/threeline; mkdir -p $OUT
ROUNDS=${ROUNDS:-5}; REPS=${REPS:-3}
MEMS=${MEMS:-"8MB 32MB 128MB 320MB 1024MB 2048MB"}

echo RUNNING > ~/Ameen/threeline.status
beat(){ echo "$(date +%H:%M:%S) $1" > ~/Ameen/threeline.heartbeat; }
fail(){ echo "$1" >&2; echo FAILED > ~/Ameen/threeline.status; exit 1; }

dep(){ local tag=$1; shift
  ./deploy.sh "$@" > $OUT/deploy-$tag.log 2>&1 || fail "DEPLOY FAILED $tag"
  grep -q 'jar verified' $OUT/deploy-$tag.log || fail "jar not verified $tag"; }

# $1=dataverse $2=sortmemory -> seconds
qt(){ curl -s -o /dev/null -m 3600 -w '%{time_total}' "$Q" --data-urlencode \
  "statement=USE $1; SET \`compiler.sortmemory\` \"$2\"; SELECT VALUE count(*) FROM (SELECT VALUE r FROM ds AS r ORDER BY r.k) AS x;"; }

# Sanity: both datasets must exist and be the same size, or the comparison is meaningless.
cnt(){ curl -s -m 600 "$Q" --data-urlencode "statement=SELECT VALUE count(*) FROM $1.ds;" \
       | grep -o '"results":[^]]*' | grep -o '[0-9]\+' | tail -1; }
NT=$(cnt test); NY=$(cnt typed)
echo "rows: test=$NT typed=$NY" | tee $OUT/rows.txt
[ -n "$NT" ] && [ "$NT" = "$NY" ] && [ "$NT" -gt 1000000 ] || fail "dataset mismatch/missing: test=$NT typed=$NY"

: > $OUT/times.txt
for ROUND in $(seq 1 $ROUNDS); do
  for ARM in stock stock-typed ours; do
    beat "round$ROUND $ARM deploy"
    case $ARM in
      stock)       dep "$ARM-r$ROUND" --jar $JARDIR/master.jar   --broker stock --heap 6g; DV=test  ;;
      stock-typed) dep "$ARM-r$ROUND" --jar $JARDIR/master.jar   --broker stock --heap 6g; DV=typed ;;
      ours)        dep "$ARM-r$ROUND" --jar $JARDIR/adaptive.jar --broker none  --heap 6g \
                       --kway true --merge-fan-in 1000000 --cap-mult 1 --auto-type-key true; DV=test ;;
    esac

    for MEM in $MEMS; do
      beat "round$ROUND $ARM $MEM"
      S=$(cat $CL/logs/nc-*.log 2>/dev/null | wc -l)
      qt $DV $MEM >/dev/null                                    # warm up this cell
      for r in $(seq 1 $REPS); do
        echo "TL $ROUND $ARM $MEM $(qt $DV $MEM)" >> $OUT/times.txt
      done
      # Honesty check: record what the sorter ACTUALLY did with the key on this arm.
      cat $CL/logs/nc-*.log 2>/dev/null | tail -n +$((S+1)) \
        | grep -o 'adaptive-sort-keys:.*' | head -1 | sed "s/^/KEYS $ARM $MEM /" >> $OUT/keys.txt
    done
  done
  echo "  round $ROUND complete: $(wc -l < $OUT/times.txt) samples" | tee -a $OUT/progress.txt
done

echo DONE > ~/Ameen/threeline.status
beat "done"
