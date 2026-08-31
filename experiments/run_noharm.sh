#!/usr/bin/env bash
# E1 "no harm": stock vs adaptive(k-way) across four sort-memory levels, 40 trials per cell.
#
# Design notes:
#  - Arms ALTERNATE within each round so machine drift cannot be read as a treatment effect.
#  - 4 rounds x 10 reps = 40 trials per cell (n=40 gives SE ~= sd/6.3; at sd~0.2s on a 20s query
#    that resolves ~0.5% differences).
#  - The adaptive arm uses --cap-mult 1: with an inert broker there is no growth to accommodate,
#    and the default 4x multiplier overflows the int pool budget above ~512MB sort memory.
#  - A failed deploy is FATAL and the jar md5 is verified, because an earlier matrix silently
#    measured the stock jar under seven different labels.
set -uo pipefail
cd "$(dirname "$0")"
export REPO_ROOT=/mnt/nvme/asterixdb JARDIR=/mnt/nvme/jars
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64
CL=$REPO_ROOT/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT/opt/local
Q=http://127.0.0.1:19002/query/service
OUT=/mnt/nvme/noharm; mkdir -p $OUT
ROUNDS=${ROUNDS:-4}; REPS=${REPS:-10}
LEVELS=${LEVELS:-"3200KB 32MB 320MB 2048MB"}
echo RUNNING > /mnt/nvme/noharm.status
beat(){ echo "$1" > /mnt/nvme/noharm.heartbeat; }
fail(){ echo "$1" >&2; echo FAILED > /mnt/nvme/noharm.status; exit 1; }

deploy_arm(){ # $1=arm $2=tag
  local n=$1
  if [ "$n" = stock ]; then
    ./deploy.sh --jar $JARDIR/master.jar --broker stock --heap 8g > $OUT/deploy-$2.log 2>&1 \
      || fail "DEPLOY FAILED stock $2"
  else
    ./deploy.sh --jar $JARDIR/adaptive.jar --broker none --heap 8g --kway true --cap-mult 1 \
      > $OUT/deploy-$2.log 2>&1 || fail "DEPLOY FAILED adaptive $2"
  fi
  grep -q 'jar verified' $OUT/deploy-$2.log || fail "jar not verified for $2"
}
qtime(){ curl -s -o /dev/null -m 1800 -w '%{time_total}' "$Q" \
  --data-urlencode "statement=USE test; SET \`compiler.sortmemory\` \"$1\"; SELECT VALUE count(*) FROM (SELECT VALUE r FROM ds AS r ORDER BY r.k) AS x;"; }

echo "=== PROBE: one query per level per arm, to size the run ===" | tee $OUT/probe.txt
for MEM in $LEVELS; do
  for ARM in stock adaptive; do
    beat "probe-$MEM-$ARM"; deploy_arm $ARM "probe-$MEM-$ARM"
    echo "PROBE $ARM $MEM $(qtime $MEM)" | tee -a $OUT/probe.txt
  done
done

echo "=== MAIN: $ROUNDS rounds x $REPS reps = $((ROUNDS*REPS)) trials per cell ===" | tee $OUT/timings.txt
for round in $(seq 1 $ROUNDS); do
  for MEM in $LEVELS; do
    for ARM in stock adaptive; do
      beat "r$round-$MEM-$ARM"
      deploy_arm $ARM "$MEM-$ARM"
      qtime $MEM >/dev/null   # warm
      for r in $(seq 1 $REPS); do
        echo "TIME $ARM $MEM r${round}_$r $(qtime $MEM)" >> $OUT/timings.txt
      done
      echo "  done r$round $MEM $ARM" >&2
    done
  done
done
beat done; echo DONE > /mnt/nvme/noharm.status; echo NOHARM_COMPLETE
