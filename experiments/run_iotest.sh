#!/usr/bin/env bash
# Does bucketing pay once I/O is REAL?
#
# Every number so far is page-cache resident: 859MB of data against ~38GB of cache, so no run ever
# reaches the device. That setup can only show what bucketing COSTS (extra cascade work) and is
# structurally incapable of showing what it SAVES (sort work overlapped with arrival, fig. 1).
#
# Two pressures are applied together:
#   balloon      - pins RAM so the kernel cannot keep the dataset cached during the query
#   fadvise drop - drops the dataset's cached pages before each timed query, so every rep reads cold
#
# Arms are the two that matter for the claim: stock as shipped, and everything of ours turned on.
set -uo pipefail
cd "$(dirname "$0")"
export REPO_ROOT=~/Ameen/asterixdb JARDIR=~/Ameen/jars
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
ASM=$REPO_ROOT/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT
CL=$ASM/opt/local
Q=http://127.0.0.1:19002/query/service
OUT=~/Ameen/iotest-${TAG:-small}; mkdir -p $OUT
ROUNDS=${ROUNDS:-3}; REPS=${REPS:-2}
MEMS=${MEMS:-"32MB 512MB 2048MB"}
DV=${DV:-test}          # which dataverse to sort
TAG=${TAG:-small}       # names the output dir, so runs at different scales do not overwrite
BALLOON_GB=${BALLOON_GB:-40}

echo RUNNING > ~/Ameen/iotest-${TAG:-small}.status
beat(){ echo "$(date +%H:%M:%S) $1" > ~/Ameen/iotest-${TAG:-small}.heartbeat; }
fail(){ echo "$1" >&2; echo FAILED > ~/Ameen/iotest-${TAG:-small}.status; cleanup; exit 1; }
cleanup(){ [ -n "${BPID:-}" ] && kill "$BPID" 2>/dev/null; }
trap cleanup EXIT INT TERM
dep(){ local tag=$1; shift
  ./deploy.sh "$@" > $OUT/deploy-$tag.log 2>&1 || fail "DEPLOY FAILED $tag"
  grep -q 'jar verified' $OUT/deploy-$tag.log || fail "jar not verified $tag"; }
qt(){ curl -s -o /dev/null -m 7200 -w '%{time_total}' "$Q" --data-urlencode \
  "statement=USE $1; SET \`compiler.sortmemory\` \"$2\"; SELECT VALUE count(*) FROM (SELECT VALUE r FROM ds AS r ORDER BY r.k) AS x;"; }

# Record the cache state so a reader can tell the pressure was real, not assumed.
cachestat(){ free -g | awk '/^Mem:/{printf "total=%s used=%s free=%s cache=%s", $2,$3,$4,$6}'; }

echo "before balloon: $(cachestat)" | tee $OUT/mem.txt
~/Ameen/balloon "$BALLOON_GB" > $OUT/balloon.log 2>&1 &
BPID=$!
sleep 45   # let it fault in every page
grep -q "holding" $OUT/balloon.log || fail "balloon did not start: $(cat $OUT/balloon.log)"
echo "after balloon:  $(cachestat)" | tee -a $OUT/mem.txt

: > $OUT/times.txt
for ROUND in $(seq 1 $ROUNDS); do
  for ARM in stock ours; do
    beat "round$ROUND $ARM deploy"
    if [ $ARM = stock ]; then dep "$ARM-r$ROUND" --jar $JARDIR/master.jar --broker stock --heap 6g
    else dep "$ARM-r$ROUND" --jar $JARDIR/adaptive.jar --broker none --heap 6g --cap-mult 1 \
             --auto-type-key true --kway true --merge-fan-in 1000000; fi
    for MEM in $MEMS; do
      for r in $(seq 1 $REPS); do
        beat "round$ROUND $ARM $MEM rep$r"
        ./drop_caches.sh "$CL/data" >> $OUT/drop.log 2>&1 || true
        echo "IO $ROUND $ARM $MEM $(qt $DV $MEM)" >> $OUT/times.txt
      done
    done
  done
  echo "round $ROUND: $(wc -l < $OUT/times.txt) samples; $(cachestat)" | tee -a $OUT/progress.txt
done
echo "final: $(cachestat)" | tee -a $OUT/mem.txt
echo DONE > ~/Ameen/iotest-${TAG:-small}.status; beat done
