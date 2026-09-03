#!/usr/bin/env bash
# Does more memory help when run-file I/O is REAL?
#
# Every previous 7GB measurement had 31GB of page cache against a 6.8GB dataset and ~19GB of run
# files -- so run files were written to and read back from RAM. The disk cost that makes fewer,
# longer runs win was never present, which is why more memory only ever looked slightly harmful:
# comparisons are ~N*log2(runSize) + N*log2(numRuns) = N*log2(N), a constant, so with free I/O the
# only visible effect is larger runs costing more comparisons.
#
# Here a balloon squeezes the cache below the working set, so runs must actually reach the device.
# PREDICTION: with real I/O the curve should turn monotonically DOWNWARD -- more memory, fewer runs,
# fewer merge passes, less disk traffic. If it does not, the merge is genuinely cheap and the
# "longer runs are better" intuition does not hold for this sort.
set -uo pipefail
cd "$(dirname "$0")"
EXP=$(cd ../.. && pwd)
export REPO_ROOT=~/Ameen/asterixdb JARDIR=~/Ameen/jars
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
CL=$REPO_ROOT/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT/opt/local
Q=http://127.0.0.1:19002/query/service
OUT=~/Ameen/mergepass-io; mkdir -p $OUT
MEMS=${MEMS:-"8MB 16MB 32MB 64MB 128MB 512MB"}
REPS=${REPS:-2}; BALLOON_GB=${BALLOON_GB:-48}
echo RUNNING > ~/Ameen/mergepassio.status
beat(){ echo "$(date +%H:%M:%S) $1" > ~/Ameen/mergepassio.heartbeat; }
cleanup(){ [ -n "${BPID:-}" ] && kill "$BPID" 2>/dev/null; }
trap cleanup EXIT INT TERM
fail(){ echo "$1" >&2; echo FAILED > ~/Ameen/mergepassio.status; exit 1; }
dep(){ local tag=$1; shift
  (cd $EXP && ./deploy.sh "$@") > $OUT/deploy-$tag.log 2>&1 || fail "DEPLOY FAILED $tag"
  grep -q 'jar verified' $OUT/deploy-$tag.log || fail "jar not verified $tag"; }
alive(){ [ "$(curl -s -m 25 -o /dev/null -w '%{http_code}' "$Q" --data-urlencode 'statement=SELECT 1;')" = 200 ]; }
snap(){ : > $OUT/.snap; for f in $CL/logs/nc-*.log; do [ -f "$f" ] && echo "$f $(wc -l < "$f")" >> $OUT/.snap; done; }
since(){ while read -r f n; do tail -n +$((n+1)) "$f"; done < $OUT/.snap; }
qt(){ curl -s -o /dev/null -m 7200 -w '%{time_total}' "$Q" --data-urlencode \
  "statement=USE big; SET \`compiler.sortmemory\` \"$1\"; SELECT VALUE count(*) FROM (SELECT VALUE r FROM ds AS r ORDER BY r.k) AS x;"; }
cachestat(){ free -g | awk '/^Mem:/{printf "used=%sGB cache=%sGB", $3,$6}'; }

beat deploy
dep io --jar $JARDIR/adaptive.jar --broker none --heap 6g --cap-mult 1 \
    --auto-type-key true --kway true --merge-fan-in 1000000
alive || fail "cluster down after deploy"

echo "before balloon: $(cachestat)" | tee $OUT/mem.txt
~/Ameen/balloon "$BALLOON_GB" > $OUT/balloon.log 2>&1 & BPID=$!
sleep 45
grep -q holding $OUT/balloon.log || fail "balloon failed: $(cat $OUT/balloon.log)"
echo "after balloon:  $(cachestat)" | tee -a $OUT/mem.txt

: > $OUT/times.txt; : > $OUT/runs.txt
for MEM in $MEMS; do
  beat "$MEM"; snap
  alive || fail "cluster unreachable before $MEM"
  # drop the dataset's cached pages so each cell starts cold
  (cd $EXP && ./drop_caches.sh "$CL/data") >> $OUT/drop.log 2>&1 || true
  for r in $(seq 1 $REPS); do
    T=$(qt $MEM)
    awk -v t="$T" 'BEGIN{exit !(t < 1.0)}' && { alive || fail "cluster died at $MEM"; fail "implausible ${T}s at $MEM"; }
    echo "MPIO $MEM $T" >> $OUT/times.txt
  done
  since | grep -c 'adaptive-sort-run:' | sed "s/^/RUNS $MEM /" >> $OUT/runs.txt
  echo "  $MEM done: $(cachestat)" >> $OUT/mem.txt
done
echo "final: $(cachestat)" | tee -a $OUT/mem.txt
echo DONE > ~/Ameen/mergepassio.status; beat done
