#!/usr/bin/env bash
# Reclaim, with budgets that ACTUALLY straddle the single-pass threshold.
#
# The first attempt mislabelled its arms: "below threshold" reclaimed 8MB->4MB on a dataset whose
# threshold is 3.3MB, so the post-reclaim budget was still single-pass and no merge pass was ever
# added. Threshold is B < sqrt(dataPerPartition * frameSize): ~3.3MB for the 600MB sets, ~7.6MB for
# TPC-DS. A 50% cut therefore has to START just above 2x the threshold to cross it.
#
#   crosses : test 4MB->2MB (2 < 3.3)      tpcds 8MB->4MB (4 < 7.6)
#   stays   : test 64MB->32MB              tpcds 128MB->64MB
#
# Also records the STATIC time at the post-reclaim budget, so we can separate two things the first
# run conflated: the cost of ending up with less memory, versus the cost of the reclaim itself.
set -uo pipefail
cd "$(dirname "$0")"
EXP=$(cd ../.. && pwd)
export REPO_ROOT=~/Ameen/asterixdb JARDIR=~/Ameen/jars
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
CL=$REPO_ROOT/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT/opt/local
Q=http://127.0.0.1:19002/query/service
OUT=~/Ameen/reclaim2; mkdir -p $OUT
REPS=${REPS:-4}; BALLOON_GB=${BALLOON_GB:-44}
echo RUNNING > ~/Ameen/reclaim2.status
beat(){ echo "$(date +%H:%M:%S) $1" > ~/Ameen/reclaim2.heartbeat; }
cleanup(){ [ -n "${BPID:-}" ] && kill "$BPID" 2>/dev/null; }
trap cleanup EXIT INT TERM
fail(){ echo "FATAL: $1" >&2; echo FAILED > ~/Ameen/reclaim2.status; exit 1; }
dep(){ local tag=$1; shift
  (cd $EXP && ./deploy.sh "$@") > $OUT/deploy-$tag.log 2>&1 || fail "DEPLOY FAILED $tag"
  grep -q 'jar verified' $OUT/deploy-$tag.log || fail "jar not verified $tag"; }
alive(){ [ "$(curl -s -m 25 -o /dev/null -w '%{http_code}' "$Q" --data-urlencode 'statement=SELECT 1;')" = 200 ]; }
qt(){ curl -s -o /dev/null -m 7200 -w '%{time_total}' "$Q" --data-urlencode \
  "statement=USE $1; SET \`compiler.sortmemory\` \"$2\"; SELECT VALUE count(*) FROM (SELECT VALUE r FROM $3 AS r ORDER BY r.$4) AS x;"; }
run_cell(){ # tag dv mem table col   -- first rep is JIT/cluster warm-up and is DISCARDED
  local tag=$1 dv=$2 mem=$3 tb=$4 col=$5
  (cd $EXP && ./drop_caches.sh "$CL/data") >> $OUT/drop.log 2>&1 || true
  qt "$dv" "$mem" "$tb" "$col" > /dev/null
  for r in $(seq 1 $REPS); do
    (cd $EXP && ./drop_caches.sh "$CL/data") >> $OUT/drop.log 2>&1 || true
    local T; T=$(qt "$dv" "$mem" "$tb" "$col")
    awk -v t="$T" 'BEGIN{exit !(t < 1.0)}' && { alive || fail "cluster died: $tag"; fail "implausible ${T}s: $tag"; }
    echo "$tag $mem $T" >> $OUT/times.txt
  done
}
echo "before balloon: $(free -g | awk '/^Mem:/{print "cache="$6"GB"}')" | tee $OUT/mem.txt
~/Ameen/balloon "$BALLOON_GB" > $OUT/balloon.log 2>&1 & BPID=$!
sleep 45; grep -q holding $OUT/balloon.log || fail "balloon did not start"
echo "after balloon:  $(free -g | awk '/^Mem:/{print "cache="$6"GB"}')" | tee -a $OUT/mem.txt

: > $OUT/times.txt
BASE="--heap 6g --cap-mult 1 --auto-type-key true --kway true --merge-fan-in 1000000"
#          dv     table          col               start  post   label
for SPEC in "test:ds:k:4MB:2MB:crosses" "test:ds:k:64MB:32MB:stays" \
            "tpcds:store_sales:ss_sales_price:8MB:4MB:crosses" \
            "tpcds:store_sales:ss_sales_price:128MB:64MB:stays"; do
  IFS=: read DV TB COL HI LO LBL <<< "$SPEC"
  # 1) static at the STARTING budget  2) reclaim 50% from it  3) static at the POST-reclaim budget
  beat "$DV $LBL static-hi"; dep "$DV-$LBL-static" --jar $JARDIR/adaptive.jar --broker none $BASE
  alive || fail "cluster down"; run_cell "static $DV $LBL" "$DV" "$HI" "$TB" "$COL"
  beat "$DV $LBL static-lo"; run_cell "staticlo $DV $LBL" "$DV" "$LO" "$TB" "$COL"
  beat "$DV $LBL reclaim"
  dep "$DV-$LBL-reclaim" --jar $JARDIR/adaptive.jar --broker scripted \
      --script "$(pwd)/brokers/moderate.csv" $BASE --release-on-shrink true
  alive || fail "cluster down"; run_cell "reclaim $DV $LBL" "$DV" "$HI" "$TB" "$COL"
done
echo DONE > ~/Ameen/reclaim2.status; beat done
