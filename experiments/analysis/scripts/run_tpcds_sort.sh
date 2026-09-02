#!/usr/bin/env bash
# Sort a REAL TPC-DS fact table, and show what the official schema costs stock.
#
# The AsterixDB-shipped TPC-DS schema declares nearly every store_sales column NULLABLE
# (double?/bigint?). A nullable type is a union, which the normalized-key provider has no case for,
# so it returns null and the sort runs with NO normalized key. Sorting ss_sales_price therefore hits
# the hole on a standard benchmark, not on data we invented.
#
# CONTROL: `tpcdsnn` holds the same rows with the sort column coalesced and declared NON-nullable --
# i.e. what stock could achieve if the schema had avoided `?`. Same row count, same distribution
# apart from nulls, so the gap between them is attributable to the declaration alone.
set -uo pipefail
cd "$(dirname "$0")"
EXP=$(cd ../.. && pwd)
export REPO_ROOT=~/Ameen/asterixdb JARDIR=~/Ameen/jars
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
CL=$REPO_ROOT/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT/opt/local
Q=http://127.0.0.1:19002/query/service
OUT=~/Ameen/tpcdssort; mkdir -p $OUT
MEMS=${MEMS:-"32MB 128MB 512MB 2048MB"}; ROUNDS=${ROUNDS:-2}; REPS=${REPS:-3}
echo RUNNING > ~/Ameen/tpcdssort.status
beat(){ echo "$(date +%H:%M:%S) $1" > ~/Ameen/tpcdssort.heartbeat; }
fail(){ echo "$1" >&2; echo FAILED > ~/Ameen/tpcdssort.status; exit 1; }
dep(){ local tag=$1; shift
  (cd $EXP && ./deploy.sh "$@") > $OUT/deploy-$tag.log 2>&1 || fail "DEPLOY FAILED $tag"
  grep -q 'jar verified' $OUT/deploy-$tag.log || fail "jar not verified $tag"; }
sql(){ curl -s -m 7200 -o $OUT/.out -w '%{http_code}' "$Q" --data-urlencode "statement=$1"; }
cnt(){ curl -s -m 1800 "$Q" --data-urlencode "statement=SELECT VALUE count(*) FROM $1.store_sales;" \
       | grep -o '"results":[^]]*' | grep -o '[0-9]\+' | tail -1; }
qt(){ curl -s -o /dev/null -m 7200 -w '%{time_total}' "$Q" --data-urlencode \
  "statement=USE $1; SET \`compiler.sortmemory\` \"$2\"; SELECT VALUE count(*) FROM (SELECT VALUE r FROM store_sales AS r ORDER BY r.ss_sales_price) AS x;"; }
snap(){ : > $OUT/.snap; for f in $CL/logs/nc-*.log; do [ -f "$f" ] && echo "$f $(wc -l < "$f")" >> $OUT/.snap; done; }
since(){ while read -r f n; do tail -n +$((n+1)) "$f"; done < $OUT/.snap; }

N0=$(cnt tpcds); echo "tpcds.store_sales rows=$N0" | tee $OUT/rows.txt
[ "$N0" -gt 1000000 ] || fail "tpcds not loaded ($N0 rows)"

# The non-nullable control (tpcdsnn2, all 23 columns) is built by build_tpcds_control.sh so that
# it differs from tpcds ONLY in nullability -- same row count, same columns, same payload width.
[ "$(cnt tpcdsnn2 2>/dev/null || echo 0)" = "$N0" ] || fail "tpcdsnn2 control missing or wrong size"

: > $OUT/times.txt; : > $OUT/keys.txt
for ROUND in $(seq 1 $ROUNDS); do
  # stock-nullable  : official schema, the hole
  # stock-nonnull   : same data, declaration fixed -- stock at its best
  # ours-nullable   : official schema + auto-detection
  for ARM in stock-nullable stock-nonnull ours-nullable; do
    beat "round$ROUND $ARM"
    case $ARM in
      stock-nullable) dep "$ARM-r$ROUND" --jar $JARDIR/master.jar --broker stock --heap 6g;  DV=tpcds   ;;
      stock-nonnull)  dep "$ARM-r$ROUND" --jar $JARDIR/master.jar --broker stock --heap 6g;  DV=tpcdsnn2 ;;
      ours-nullable)  dep "$ARM-r$ROUND" --jar $JARDIR/adaptive.jar --broker none --heap 6g --cap-mult 1 \
                          --auto-type-key true --kway true --merge-fan-in 1000000;            DV=tpcds   ;;
    esac
    for MEM in $MEMS; do
      beat "round$ROUND $ARM $MEM"; snap
      qt $DV $MEM >/dev/null
      for r in $(seq 1 $REPS); do echo "TS $ROUND $ARM $MEM $(qt $DV $MEM)" >> $OUT/times.txt; done
      since | grep -o 'adaptive-sort-keys:.*' | head -1 | sed "s/^/KEY $ARM $MEM /" >> $OUT/keys.txt
    done
  done
  echo "round $ROUND: $(wc -l < $OUT/times.txt) samples" | tee -a $OUT/progress.txt
done
echo DONE > ~/Ameen/tpcdssort.status; beat done
