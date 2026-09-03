#!/usr/bin/env bash
# Overnight: every headline claim re-measured under REAL I/O pressure.
#
# Everything before this was cache-resident (31GB cache vs a 0.9-7GB working set), so run files
# never reached disk and "more memory" could not possibly pay. Here a balloon squeezes the cache and
# every timed query is preceded by a cache drop.
#
# Memory levels BRACKET each dataset's single-pass threshold, B < sqrt(dataPerPartition * frameSize):
#   test/mixbig (325MB/partition) -> ~3.3MB      tpcds (1.7GB/partition) -> ~7.6MB
# Testing only above the threshold (as every previous sweep did) can only ever show "memory does not
# help", because within a single pass the merge moves the same bytes regardless of run count.
#
# S  sweep: stock vs type-fix-only vs bucketed, across memory   -> U-curve, memory benefit, no-harm
# N  neighbor: sort work spread across arrival (spreadPct)
# R  reclaim: giving memory back ABOVE vs BELOW the threshold
set -uo pipefail
cd "$(dirname "$0")"
EXP=$(cd ../.. && pwd)
export REPO_ROOT=~/Ameen/asterixdb JARDIR=~/Ameen/jars
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
CL=$REPO_ROOT/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT/opt/local
Q=http://127.0.0.1:19002/query/service
OUT=~/Ameen/overnight; mkdir -p $OUT
REPS=${REPS:-3}; BALLOON_GB=${BALLOON_GB:-44}
NOBUCKET=2000000000

echo RUNNING > ~/Ameen/overnight.status
beat(){ echo "$(date +%H:%M:%S) $1" > ~/Ameen/overnight.heartbeat; }
cleanup(){ [ -n "${BPID:-}" ] && kill "$BPID" 2>/dev/null; }
trap cleanup EXIT INT TERM
fail(){ echo "FATAL: $1" >&2; echo FAILED > ~/Ameen/overnight.status; exit 1; }
note(){ echo "$(date +%H:%M:%S) $*" >> $OUT/log.txt; }
dep(){ local tag=$1; shift
  (cd $EXP && ./deploy.sh "$@") > $OUT/deploy-$tag.log 2>&1 || fail "DEPLOY FAILED $tag"
  grep -q 'jar verified' $OUT/deploy-$tag.log || fail "jar not verified $tag"; }
alive(){ [ "$(curl -s -m 25 -o /dev/null -w '%{http_code}' "$Q" --data-urlencode 'statement=SELECT 1;')" = 200 ]; }
snap(){ : > $OUT/.snap; for f in $CL/logs/nc-*.log; do [ -f "$f" ] && echo "$f $(wc -l < "$f")" >> $OUT/.snap; done; }
since(){ while read -r f n; do tail -n +$((n+1)) "$f"; done < $OUT/.snap; }
drop(){ (cd $EXP && ./drop_caches.sh "$CL/data") >> $OUT/drop.log 2>&1 || true; }
qt(){ curl -s -o /dev/null -m 7200 -w '%{time_total}' "$Q" --data-urlencode \
  "statement=USE $1; SET \`compiler.sortmemory\` \"$2\"; SELECT VALUE count(*) FROM (SELECT VALUE r FROM $3 AS r ORDER BY r.$4) AS x;"; }

# Timed query with a cold cache. A sub-1s result means the request never ran; if the cluster is
# alive that budget was simply rejected, so record SKIP and continue rather than losing the night.
measure(){ # $1=tag $2=dv $3=mem $4=table $5=col
  drop
  local T; T=$(qt "$2" "$3" "$4" "$5")
  if awk -v t="$T" 'BEGIN{exit !(t < 1.0)}'; then
    alive || fail "cluster died: $1 @ $3"
    note "SKIP $1 $3 (returned ${T}s -- budget likely rejected)"; return 1
  fi
  echo "$1 $3 $T" >> $OUT/times.txt; return 0
}

echo "before balloon: $(free -g | awk '/^Mem:/{print "cache="$6"GB"}')" | tee $OUT/mem.txt
~/Ameen/balloon "$BALLOON_GB" > $OUT/balloon.log 2>&1 & BPID=$!
sleep 45
grep -q holding $OUT/balloon.log || fail "balloon did not start: $(cat $OUT/balloon.log)"
echo "after balloon:  $(free -g | awk '/^Mem:/{print "cache="$6"GB"}')" | tee -a $OUT/mem.txt

: > $OUT/times.txt; : > $OUT/spread.txt; : > $OUT/log.txt
BASE="--broker none --heap 6g --cap-mult 1"

# ---------------- S: U-curve / memory benefit / no-harm ----------------
# datasets as "dv:table:col:mems"
for SPEC in "test:ds:k:2MB 4MB 8MB 32MB 128MB 512MB" \
            "mixbig:ds:k:2MB 4MB 8MB 32MB 128MB 512MB" \
            "tpcds:store_sales:ss_sales_price:4MB 8MB 16MB 64MB 256MB 512MB"; do
  DV=${SPEC%%:*}; R=${SPEC#*:}; TB=${R%%:*}; R=${R#*:}; COL=${R%%:*}; MEMS=${R#*:}
  for ARM in stock nobucket bucket; do
    beat "S $DV $ARM"; note "S $DV $ARM"
    case $ARM in
      stock)    dep "S-$DV-$ARM" --jar $JARDIR/master.jar --broker stock --heap 6g ;;
      nobucket) dep "S-$DV-$ARM" --jar $JARDIR/adaptive.jar $BASE --auto-type-key true --bucket-tuples $NOBUCKET ;;
      bucket)   dep "S-$DV-$ARM" --jar $JARDIR/adaptive.jar $BASE --auto-type-key true --kway true --merge-fan-in 1000000 ;;
    esac
    alive || fail "cluster down after deploy S-$DV-$ARM"
    for MEM in $MEMS; do
      beat "S $DV $ARM $MEM"
      measure "S $DV $ARM" "$DV" "$MEM" "$TB" "$COL" || continue
      for r in $(seq 2 $REPS); do measure "S $DV $ARM" "$DV" "$MEM" "$TB" "$COL" || break; done
    done
  done
done

# ---------------- N: neighbor property ----------------
for SPEC in "test:ds:k:32MB" "tpcds:store_sales:ss_sales_price:64MB"; do
  DV=${SPEC%%:*}; R=${SPEC#*:}; TB=${R%%:*}; R=${R#*:}; COL=${R%%:*}; MEM=${R#*:}
  for ARM in bucketed flat; do
    beat "N $DV $ARM"; note "N $DV $ARM"
    if [ $ARM = bucketed ]; then
      dep "N-$DV-$ARM" --jar $JARDIR/adaptive.jar $BASE --auto-type-key true --kway true \
          --merge-fan-in 1000000 --phase-log true
    else
      dep "N-$DV-$ARM" --jar $JARDIR/adaptive.jar $BASE --auto-type-key true --phase-log true \
          --bucket-tuples $NOBUCKET
    fi
    alive || fail "cluster down after deploy N-$DV-$ARM"
    for r in $(seq 1 $REPS); do
      beat "N $DV $ARM rep$r"; snap
      measure "N $DV $ARM" "$DV" "$MEM" "$TB" "$COL" || continue
      since | grep -o 'adaptive-sort-phase:.*' | grep -oE 'spreadPct=[0-9]+|sortEvents=[0-9]+' \
        | paste - - | sed "s/^/SPREAD $DV $ARM /" >> $OUT/spread.txt
    done
  done
done

# ---------------- R: reclaim above vs below the single-pass threshold ----------------
# `above` starts well clear of the threshold so a 50% cut stays single-pass;
# `below` starts just above it so the same cut crosses into a second merge pass.
for SPEC in "test:ds:k:64MB:8MB" "tpcds:store_sales:ss_sales_price:128MB:16MB"; do
  DV=${SPEC%%:*}; R=${SPEC#*:}; TB=${R%%:*}; R=${R#*:}; COL=${R%%:*}; R=${R#*:}
  HI=${R%%:*}; LO=${R#*:}
  for ARM in none-hi reclaim-hi none-lo reclaim-lo; do
    case $ARM in
      none-hi)    MEM=$HI; SCRIPTARG="--broker none" ;;
      reclaim-hi) MEM=$HI; SCRIPTARG="--broker scripted --script $(pwd)/brokers/moderate.csv" ;;
      none-lo)    MEM=$LO; SCRIPTARG="--broker none" ;;
      reclaim-lo) MEM=$LO; SCRIPTARG="--broker scripted --script $(pwd)/brokers/moderate.csv" ;;
    esac
    beat "R $DV $ARM"; note "R $DV $ARM @ $MEM"
    dep "R-$DV-$ARM" --jar $JARDIR/adaptive.jar --heap 6g --cap-mult 1 $SCRIPTARG \
        --auto-type-key true --kway true --merge-fan-in 1000000 --release-on-shrink true
    alive || fail "cluster down after deploy R-$DV-$ARM"
    for r in $(seq 1 $REPS); do measure "R $DV $ARM" "$DV" "$MEM" "$TB" "$COL" || break; done
  done
done

echo "final: $(free -g | awk '/^Mem:/{print "cache="$6"GB"}')" | tee -a $OUT/mem.txt
echo DONE > ~/Ameen/overnight.status; beat done
