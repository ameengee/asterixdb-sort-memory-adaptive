#!/usr/bin/env bash
# Why does OUR arm get slower with more memory when stock-typed stays flat?
# Three suspects, separated by turning exactly one thing off at a time:
#
#   A ours-full      untyped + auto-key + bucketing   (the shipped config; rises ~32%)
#   B autokey-only   untyped + auto-key + NO bucketing (1 giant bucket) -> is the key to blame?
#   C bucket-only    typed   + native key + bucketing  -> is bucketing to blame?
#   D neither        typed   + native key + NO bucketing -> must match stock-typed, sanity check
#
# If B is flat and C rises, bucketing is the cause. If B rises, the auto-key is.
set -uo pipefail
cd "$(dirname "$0")"
export REPO_ROOT=~/Ameen/asterixdb JARDIR=~/Ameen/jars
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
CL=$REPO_ROOT/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT/opt/local
Q=http://127.0.0.1:19002/query/service
OUT=~/Ameen/whyslow; mkdir -p $OUT
ROUNDS=${ROUNDS:-4}; REPS=${REPS:-3}
MEMS=${MEMS:-"8MB 32MB 320MB 2048MB"}
NOBUCKET=2000000000   # one bucket per run == bucketing effectively off

echo RUNNING > ~/Ameen/whyslow.status
beat(){ echo "$(date +%H:%M:%S) $1" > ~/Ameen/whyslow.heartbeat; }
fail(){ echo "$1" >&2; echo FAILED > ~/Ameen/whyslow.status; exit 1; }
dep(){ local tag=$1; shift
  ./deploy.sh "$@" > $OUT/deploy-$tag.log 2>&1 || fail "DEPLOY FAILED $tag"
  grep -q 'jar verified' $OUT/deploy-$tag.log || fail "jar not verified $tag"; }
qt(){ curl -s -o /dev/null -m 3600 -w '%{time_total}' "$Q" --data-urlencode \
  "statement=USE $1; SET \`compiler.sortmemory\` \"$2\"; SELECT VALUE count(*) FROM (SELECT VALUE r FROM ds AS r ORDER BY r.k) AS x;"; }

: > $OUT/times.txt
COMMON="--jar $JARDIR/adaptive.jar --broker none --heap 6g --kway true --merge-fan-in 1000000 --cap-mult 1"
for ROUND in $(seq 1 $ROUNDS); do
  for ARM in ours-full autokey-only bucket-only neither; do
    beat "round$ROUND $ARM deploy"
    case $ARM in
      ours-full)    dep "$ARM-r$ROUND" $COMMON --auto-type-key true;                            DV=test  ;;
      autokey-only) dep "$ARM-r$ROUND" $COMMON --auto-type-key true --bucket-tuples $NOBUCKET;  DV=test  ;;
      bucket-only)  dep "$ARM-r$ROUND" $COMMON;                                                 DV=typed ;;
      neither)      dep "$ARM-r$ROUND" $COMMON --bucket-tuples $NOBUCKET;                       DV=typed ;;
    esac
    for MEM in $MEMS; do
      beat "round$ROUND $ARM $MEM"
      S=$(cat $CL/logs/nc-*.log 2>/dev/null | wc -l)
      qt $DV $MEM >/dev/null
      for r in $(seq 1 $REPS); do echo "WS $ROUND $ARM $MEM $(qt $DV $MEM)" >> $OUT/times.txt; done
      # record buckets/run + phase split so the mechanism is visible, not just the timing
      cat $CL/logs/nc-*.log 2>/dev/null | tail -n +$((S+1)) \
        | grep -oE 'adaptive-sort-(keys|run|phase):.*' | head -4 | sed "s/^/EV $ARM $MEM /" >> $OUT/events.txt
    done
  done
  echo "round $ROUND done: $(wc -l < $OUT/times.txt) samples" | tee -a $OUT/progress.txt
done
echo DONE > ~/Ameen/whyslow.status
beat done
