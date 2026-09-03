#!/usr/bin/env bash
# What ARE the right cascade fan-in and bucket size? We picked 1000000 and 256 buckets from partial
# evidence and never swept them properly.
#
# CORRECTED PARAMETERISATION. The first attempt swept `bucketCountTarget`, which sets
# bucketTargetBytes = max(256KB, budget/count) -- so it is bucket SIZE expressed relative to the
# budget. At 8MB and 32MB the 256KB floor dominated and every count collapsed to the SAME actual
# bucket size, making two of the three rows uninformative by construction. Sweeping ABSOLUTE bucket
# bytes with the scaling policy disabled (--bucket-count 0) decouples size from budget, so a given
# bucket size means the same thing at every budget.
#
# Strategy: a full 3-D sweep (fan-in x bucket-size x memory) is cheap on the 600MB dataset (~4s per
# query) and unaffordable on 7GB (~150s). So explore broadly here, then verify only the winners at
# scale -- rather than guessing at 7GB with a handful of points.
#
# Fan-in here is the BUCKET CASCADE width, not the final run merge (that one is set by the frame
# budget). Very large values mean "merge all buckets in one k-way pass".
set -uo pipefail
cd "$(dirname "$0")"
EXP=$(cd ../.. && pwd)
export REPO_ROOT=~/Ameen/asterixdb JARDIR=~/Ameen/jars
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
Q=http://127.0.0.1:19002/query/service
OUT=~/Ameen/tuning2; mkdir -p $OUT
DV=${DV:-test}
FANINS=${FANINS:-"2 8 32 128 1024 1000000"}
BUCKETBYTES=${BUCKETBYTES:-"65536 262144 1048576 4194304 16777216"}
MEMS=${MEMS:-"8MB 32MB 512MB"}
REPS=${REPS:-3}; ROUNDS=${ROUNDS:-1}

echo RUNNING > ~/Ameen/tuning2.status
beat(){ echo "$(date +%H:%M:%S) $1" > ~/Ameen/tuning2.heartbeat; }
fail(){ echo "$1" >&2; echo FAILED > ~/Ameen/tuning2.status; exit 1; }
dep(){ local tag=$1; shift
  (cd $EXP && ./deploy.sh "$@") > $OUT/deploy-$tag.log 2>&1 || fail "DEPLOY FAILED $tag"
  grep -q 'jar verified' $OUT/deploy-$tag.log || fail "jar not verified $tag"; }
alive(){ [ "$(curl -s -m 20 -o /dev/null -w '%{http_code}' "$Q" --data-urlencode 'statement=SELECT 1;')" = 200 ]; }
qt(){ curl -s -o /dev/null -m 3600 -w '%{time_total}' "$Q" --data-urlencode \
  "statement=USE $1; SET \`compiler.sortmemory\` \"$2\"; SELECT VALUE count(*) FROM (SELECT VALUE r FROM ds AS r ORDER BY r.k) AS x;"; }

: > $OUT/times.txt
for ROUND in $(seq 1 $ROUNDS); do
  for FI in $FANINS; do
    for BB in $BUCKETBYTES; do
      beat "round$ROUND fanin=$FI bucketBytes=$BB deploy"
      dep "fi$FI-bb$BB-r$ROUND" --jar $JARDIR/adaptive.jar --broker none --heap 6g --cap-mult 1 \
          --auto-type-key true --kway true --merge-fan-in $FI --bucket-count 0 --bucket-bytes $BB
      alive || fail "cluster down after deploy fi=$FI bc=$BC"
      for MEM in $MEMS; do
        beat "round$ROUND fanin=$FI bucketBytes=$BB @ $MEM"
        qt $DV $MEM >/dev/null   # warm-up, discarded
        for r in $(seq 1 $REPS); do
          T=$(qt $DV $MEM)
          # a 10M-row sort cannot finish in under a second; that would be a failed request
          awk -v t="$T" 'BEGIN{exit !(t < 0.5)}' && fail "implausible ${T}s at fi=$FI bc=$BC $MEM"
          echo "TU $ROUND $FI $BB $MEM $T" >> $OUT/times.txt
        done
      done
    done
  done
done
echo DONE > ~/Ameen/tuning2.status; beat done
