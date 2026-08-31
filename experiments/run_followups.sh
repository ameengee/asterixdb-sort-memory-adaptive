#!/usr/bin/env bash
# Three follow-ups, run back-to-back so the instance is not idle:
#  A) 320MB diagnostic  -- instrumented runs/frames per arm across a finer memory range, to explain
#                          why BOTH adaptive arms bump at 320MB (bimodality already ruled out)
#  B) fan-in x strategy -- figure 3 must be redone: its "fan-in does not matter" claim was measured
#                          with pairwise merging only, and with k-way the fan-in matters a lot
#  C) 512KB / 1MB       -- is stock's curve a U, or monotonic degradation over the whole range?
set -uo pipefail
cd "$(dirname "$0")"
export REPO_ROOT=/mnt/nvme/asterixdb JARDIR=/mnt/nvme/jars
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64
CL=$REPO_ROOT/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT/opt/local
Q=http://127.0.0.1:19002/query/service
OUT=/mnt/nvme/followups; mkdir -p $OUT
echo RUNNING > /mnt/nvme/followups.status
beat(){ echo "$1" > /mnt/nvme/followups.heartbeat; }
fail(){ echo "$1" >&2; echo FAILED > /mnt/nvme/followups.status; exit 1; }
dep(){ # $1=tag, rest=deploy args
  local tag=$1; shift
  ./deploy.sh "$@" > $OUT/deploy-$tag.log 2>&1 || fail "DEPLOY FAILED $tag"
  grep -q 'jar verified' $OUT/deploy-$tag.log || fail "jar not verified $tag"
}
qt(){ curl -s -o /dev/null -m 1800 -w '%{time_total}' "$Q" \
  --data-urlencode "statement=USE test; SET \`compiler.sortmemory\` \"$1\"; SELECT VALUE count(*) FROM (SELECT VALUE r FROM ds AS r ORDER BY r.k) AS x;"; }

# ---------- A: run structure across a finer memory range ----------
echo "=== A: runs/frames per level (instrumented) ===" | tee $OUT/diag.txt
for MEM in 32MB 128MB 256MB 320MB 512MB 1024MB 2048MB; do
  for ARM in eager kway; do
    FI=2; [ $ARM = kway ] && FI=1000000
    beat "A-$MEM-$ARM"
    dep "A-$MEM-$ARM" --jar $JARDIR/adaptive.jar --broker none --heap 8g --kway true \
        --merge-fan-in $FI --cap-mult 1 --phase-log true
    qt $MEM >/dev/null; qt $MEM >/dev/null      # two warmups: the adaptive jar needs them
    B=$(cat $CL/logs/nc-*.log 2>/dev/null | wc -l)
    T=$(qt $MEM)
    echo "DIAG $ARM $MEM time=$T" | tee -a $OUT/diag.txt
    cat $CL/logs/nc-*.log 2>/dev/null | tail -n +$((B+1)) | grep -o 'adaptive-sort-phase:.*' \
      | sed "s/^/DIAGPHASE $ARM $MEM /" | tee -a $OUT/diag.txt
  done
done

# ---------- B: fan-in x merge strategy (redo of figure 3) ----------
echo "=== B: fan-in x strategy, 10 reps each ===" | tee $OUT/fanin.txt
for MEM in 32MB 320MB; do
  for KW in false true; do
    for FI in 2 4 8 32 1000000; do
      beat "B-$MEM-kway$KW-fi$FI"
      dep "B-$MEM-$KW-$FI" --jar $JARDIR/adaptive.jar --broker none --heap 8g \
          --kway $KW --merge-fan-in $FI --cap-mult 1
      qt $MEM >/dev/null; qt $MEM >/dev/null
      for r in $(seq 1 10); do echo "FANIN $MEM kway=$KW fi=$FI $(qt $MEM)" >> $OUT/fanin.txt; done
    done
  done
done

# ---------- C: below 3.2MB -- U-curve or monotonic? ----------
echo "=== C: low-memory extension, 10 reps each ===" | tee $OUT/lowmem.txt
for MEM in 512KB 1MB 2MB; do
  for ARM in stock kway; do
    beat "C-$MEM-$ARM"
    if [ $ARM = stock ]; then dep "C-$MEM-stock" --jar $JARDIR/master.jar --broker stock --heap 8g
    else dep "C-$MEM-kway" --jar $JARDIR/adaptive.jar --broker none --heap 8g --kway true \
             --merge-fan-in 1000000 --cap-mult 1; fi
    qt $MEM >/dev/null; qt $MEM >/dev/null
    for r in $(seq 1 10); do echo "LOWMEM $ARM $MEM $(qt $MEM)" >> $OUT/lowmem.txt; done
  done
done
beat done; echo DONE > /mnt/nvme/followups.status; echo FOLLOWUPS_COMPLETE
