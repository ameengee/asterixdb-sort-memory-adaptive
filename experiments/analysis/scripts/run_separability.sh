#!/usr/bin/env bash
# How much of the win is the NORMALIZER alone, versus normalizer + our sorter hooks?
# This decides whether type detection could be contributed to AsterixDB on its own.
#
#   stock        master sorter, detection OFF          -- the baseline
#   stock+key    master sorter, detection ON           -- normalizer alone, no isKeyExact hook
#   ours         our sorter, detection ON, no bucketing-- normalizer + runtime decisiveness
set -uo pipefail
cd "$(dirname "$0")"
EXP=$(cd ../.. && pwd)
export REPO_ROOT=~/Ameen/asterixdb JARDIR=~/Ameen/jars
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
Q=http://127.0.0.1:19002/query/service
OUT=~/Ameen/separability; mkdir -p $OUT
MEMS=${MEMS:-"8MB 32MB 128MB 512MB 2048MB"}; ROUNDS=${ROUNDS:-2}; REPS=${REPS:-3}
NOBUCKET=2000000000
echo RUNNING > ~/Ameen/separability.status
beat(){ echo "$(date +%H:%M:%S) $1" > ~/Ameen/separability.heartbeat; }
fail(){ echo "$1" >&2; echo FAILED > ~/Ameen/separability.status; exit 1; }
dep(){ local tag=$1; shift
  (cd $EXP && ./deploy.sh "$@") > $OUT/deploy-$tag.log 2>&1 || fail "DEPLOY FAILED $tag"
  grep -q 'jar verified' $OUT/deploy-$tag.log || fail "jar not verified $tag"; }
qt(){ curl -s -o /dev/null -m 7200 -w '%{time_total}' "$Q" --data-urlencode \
  "statement=USE $1; SET \`compiler.sortmemory\` \"$2\"; SELECT VALUE count(*) FROM (SELECT VALUE r FROM ds AS r ORDER BY r.k) AS x;"; }
: > $OUT/times.txt
for ROUND in $(seq 1 $ROUNDS); do
  for ARM in stock stock-key ours; do
    beat "round$ROUND $ARM"
    case $ARM in
      stock)     dep "$ARM-r$ROUND" --jar $JARDIR/master.jar --broker stock --heap 6g --auto-type-key false ;;
      stock-key) dep "$ARM-r$ROUND" --jar $JARDIR/master.jar --broker stock --heap 6g --auto-type-key true ;;
      ours)      dep "$ARM-r$ROUND" --jar $JARDIR/adaptive.jar --broker none --heap 6g --cap-mult 1 \
                     --auto-type-key true --bucket-tuples $NOBUCKET ;;
    esac
    for DV in test mixbig; do
      for MEM in $MEMS; do
        beat "round$ROUND $ARM $DV @ $MEM"
        qt $DV $MEM > /dev/null
        for r in $(seq 1 $REPS); do echo "SEP $ROUND $ARM $DV $MEM $(qt $DV $MEM)" >> $OUT/times.txt; done
      done
    done
  done
done
echo DONE > ~/Ameen/separability.status; beat done
