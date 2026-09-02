#!/usr/bin/env bash
# Does a budget reduction now actually RETURN memory to the frame manager?
#
# Before this change, a reclaim lowered a number and spilled; every ByteBuffer stayed charged to the
# task, so no other query could use it. shrinkTo() releases the FREE ones. Releasing a frame that
# still holds live tuples would corrupt the sort silently, so correctness is gated FIRST and nothing
# is measured unless the output is provably still ordered.
set -uo pipefail
cd "$(dirname "$0")"
EXP=$(cd ../.. && pwd)
export REPO_ROOT=~/Ameen/asterixdb JARDIR=~/Ameen/jars
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
CL=$REPO_ROOT/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT/opt/local
Q=http://127.0.0.1:19002/query/service
OUT=~/Ameen/shrink; mkdir -p $OUT
MEMS=${MEMS:-"32MB 128MB"}; ROUNDS=${ROUNDS:-3}; REPS=${REPS:-3}
RECLAIM="--broker periodic --period 10 --action reclaim --fraction 0.5"
COMMON="--heap 6g --cap-mult 1 --auto-type-key true --kway true --merge-fan-in 1000000"

echo RUNNING > ~/Ameen/shrink.status
beat(){ echo "$(date +%H:%M:%S) $1" > ~/Ameen/shrink.heartbeat; }
fail(){ echo "$1" >&2; echo FAILED > ~/Ameen/shrink.status; exit 1; }
dep(){ local tag=$1; shift
  (cd $EXP && ./deploy.sh "$@") > $OUT/deploy-$tag.log 2>&1 || fail "DEPLOY FAILED $tag"
  grep -q 'jar verified' $OUT/deploy-$tag.log || fail "jar not verified $tag"; }
snap(){ : > $OUT/.snap; for f in $CL/logs/nc-*.log; do [ -f "$f" ] && echo "$f $(wc -l < "$f")" >> $OUT/.snap; done; }
since(){ while read -r f n; do tail -n +$((n+1)) "$f"; done < $OUT/.snap; }
qt(){ curl -s -o /dev/null -m 3600 -w '%{time_total}' "$Q" --data-urlencode \
  "statement=USE test; SET \`compiler.sortmemory\` \"$1\"; SELECT VALUE count(*) FROM (SELECT VALUE r FROM ds AS r ORDER BY r.k) AS x;"; }

# ---------- GATE: output must still be correctly ordered while frames are being released ----------
beat "correctness"
dep gate --jar $JARDIR/adaptive.jar $RECLAIM $COMMON --release-on-shrink true
curl -s -o $OUT/sorted.json -m 1800 "$Q" --data-urlencode \
  'statement=USE small; SET `compiler.sortmemory` "8MB"; SELECT VALUE r.k FROM ds AS r ORDER BY r.k;'
python3 - "$OUT/sorted.json" > $OUT/correctness.txt 2>&1 <<'PY'
import json,sys
d=json.load(open(sys.argv[1])); r=d.get("results",[])
ok = all(r[i] <= r[i+1] for i in range(len(r)-1))
print(f"rows={len(r)} status={d.get('status')} ordered={ok}")
PY
cat $OUT/correctness.txt
grep -q "ordered=True" $OUT/correctness.txt || fail "ABORT: releasing frames broke sort ORDER"
grep -q "rows=400000" $OUT/correctness.txt || fail "ABORT: wrong row count -- data lost"

# ---------- did it actually release anything? ----------
beat "release evidence"
snap; qt 32MB >/dev/null
since | grep -o 'released [0-9]* bytes back to the frame manager' | head -3 > $OUT/released.txt
COUNT=$(since | grep -c 'released .* bytes back to the frame manager' || echo 0)
echo "release log lines this query: $COUNT" | tee -a $OUT/released.txt
[ "$COUNT" -gt 0 ] || echo "WARNING: no memory was released -- shrinkTo found no free frames" >> $OUT/released.txt

# ---------- timing: does releasing cost anything? ----------
: > $OUT/times.txt
for ROUND in $(seq 1 $ROUNDS); do
  for ARM in hold release; do
    beat "round$ROUND $ARM"
    dep "$ARM-r$ROUND" --jar $JARDIR/adaptive.jar $RECLAIM $COMMON \
        --release-on-shrink $([ $ARM = release ] && echo true || echo false)
    for MEM in $MEMS; do
      beat "round$ROUND $ARM $MEM"; qt $MEM >/dev/null
      for r in $(seq 1 $REPS); do echo "SH $ROUND $ARM $MEM $(qt $MEM)" >> $OUT/times.txt; done
    done
  done
done
echo DONE > ~/Ameen/shrink.status; beat done
