#!/usr/bin/env bash
# Validate type-cut buckets: seal a bucket when the sort column's TYPE changes, so each bucket is
# single-typed and its key can be EXACT -- which lets the sorter skip the comparator on ties.
#
# CORRECTNESS RUNS FIRST and gates everything else. A key that skips the comparator when it should
# not produces silently mis-ordered output, which is the one failure we must never ship.
set -uo pipefail
cd "$(dirname "$0")"
EXP=$(cd ../.. && pwd)
export REPO_ROOT=~/Ameen/asterixdb JARDIR=~/Ameen/jars
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
CL=$REPO_ROOT/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT/opt/local
Q=http://127.0.0.1:19002/query/service
OUT=~/Ameen/typecut; mkdir -p $OUT
MEMS=${MEMS:-"32MB 512MB 2048MB"}; ROUNDS=${ROUNDS:-2}; REPS=${REPS:-3}

echo RUNNING > ~/Ameen/typecut.status
beat(){ echo "$(date +%H:%M:%S) $1" > ~/Ameen/typecut.heartbeat; }
fail(){ echo "$1" >&2; echo FAILED > ~/Ameen/typecut.status; exit 1; }
dep(){ local tag=$1; shift
  (cd $EXP && ./deploy.sh "$@") > $OUT/deploy-$tag.log 2>&1 || fail "DEPLOY FAILED $tag"
  grep -q 'jar verified' $OUT/deploy-$tag.log || fail "jar not verified $tag"; }
qt(){ curl -s -o /dev/null -m 7200 -w '%{time_total}' "$Q" --data-urlencode \
  "statement=USE $1; SET \`compiler.sortmemory\` \"$2\"; SELECT VALUE count(*) FROM (SELECT VALUE r FROM ds AS r ORDER BY r.k) AS x;"; }
snap(){ : > $OUT/.snap; for f in $CL/logs/nc-*.log; do [ -f "$f" ] && echo "$f $(wc -l < "$f")" >> $OUT/.snap; done; }
since(){ while read -r f n; do tail -n +$((n+1)) "$f"; done < $OUT/.snap; }

# ---------- GATE 1: ordering must still match stock exactly ----------
beat "correctness"
echo "=== correctness with type-cut ON ===" | tee $OUT/correctness.txt
(cd $EXP && TYPECUT_EXTRA="--type-cut true" ./test_mixed_type.sh) >> $OUT/correctness.txt 2>&1
if ! grep -q "key sequences identical: True" $OUT/correctness.txt; then
  echo "ABORT: type-cut changed sort ORDER. Benchmarks skipped." | tee -a $OUT/correctness.txt
  fail "type-cut correctness failed"
fi
grep -c "key sequences identical: True" $OUT/correctness.txt | sed 's/^/  passing ordering checks: /' | tee -a $OUT/correctness.txt

# ---------- GATE 2: it must actually engage ----------
beat "engagement"
dep engage --jar $JARDIR/adaptive.jar --broker none --heap 6g --cap-mult 1 \
   --auto-type-key true --kway true --merge-fan-in 1000000 --type-cut true
snap; qt mixbig 512MB > /dev/null
since | grep -o 'runtimeDecisive=[a-z]*' | sort | uniq -c | tee $OUT/engagement.txt
grep -q 'runtimeDecisive=true' $OUT/engagement.txt || \
  echo "WARNING: no bucket became decisive on the multi-type column; the cut may not be firing" | tee -a $OUT/engagement.txt

# ---------- benchmark: type-cut off vs on, on the MULTI-TYPE column ----------
: > $OUT/times.txt
for ROUND in $(seq 1 $ROUNDS); do
  for ARM in off on; do
    beat "round$ROUND typecut=$ARM"
    dep "tc-$ARM-r$ROUND" --jar $JARDIR/adaptive.jar --broker none --heap 6g --cap-mult 1 \
        --auto-type-key true --kway true --merge-fan-in 1000000 --type-cut $([ $ARM = on ] && echo true || echo false)
    for MEM in $MEMS; do
      beat "round$ROUND typecut=$ARM @ $MEM"
      qt mixbig $MEM > /dev/null
      for r in $(seq 1 $REPS); do echo "TC $ROUND $ARM $MEM $(qt mixbig $MEM)" >> $OUT/times.txt; done
    done
  done
done
echo DONE > ~/Ameen/typecut.status; beat done
