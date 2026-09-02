#!/usr/bin/env bash
# "Can't the user just cast the column in the query?" -- the obvious objection to the nullable
# finding. If a cast hands the compiler a concrete non-nullable type, stock would get its
# normalized key back and our contribution shrinks to a convenience.
#
# Reports ptrSize per query form. ptrSize=3 means NO normalized key; 5 or 6 means one was used.
# Timing is secondary here: the key question is whether the optimization engages at all.
set -uo pipefail
cd "$(dirname "$0")"
EXP=$(cd ../.. && pwd)
export REPO_ROOT=~/Ameen/asterixdb JARDIR=~/Ameen/jars
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
CL=$REPO_ROOT/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT/opt/local
Q=http://127.0.0.1:19002/query/service
OUT=~/Ameen/castwa; mkdir -p $OUT
echo RUNNING > ~/Ameen/castwa.status
beat(){ echo "$(date +%H:%M:%S) $1" > ~/Ameen/castwa.heartbeat; }
fail(){ echo "$1" >&2; echo FAILED > ~/Ameen/castwa.status; exit 1; }
snap(){ : > $OUT/.snap; for f in $CL/logs/nc-*.log; do [ -f "$f" ] && echo "$f $(wc -l < "$f")" >> $OUT/.snap; done; }
since(){ while read -r f n; do tail -n +$((n+1)) "$f"; done < $OUT/.snap; }

# Our jar is required just to LOG what key was chosen; auto-detection is OFF so the provider behaves
# exactly as stock does. Without this we would have no visibility into ptrSize at all.
(cd $EXP && ./deploy.sh --jar $JARDIR/adaptive.jar --broker none --heap 6g --cap-mult 1 \
   --auto-type-key false --bucket-tuples 2000000000) > $OUT/deploy.log 2>&1 || fail "deploy failed"
grep -q 'jar verified' $OUT/deploy.log || fail "jar not verified"

: > $OUT/results.txt
probe(){ # $1=label  $2=order-by expression  $3=dataverse
  beat "$1"; snap
  T=$(curl -s -o /dev/null -m 3600 -w '%{time_total}' "$Q" --data-urlencode \
    "statement=USE $3; SET \`compiler.sortmemory\` \"128MB\"; SELECT VALUE count(*) FROM (SELECT VALUE r FROM store_sales AS r ORDER BY $2) AS x;")
  K=$(since | grep -o 'adaptive-sort-keys:.*' | head -1)
  echo "$1 | time=${T}s | ${K:-<no sort-key line>}" >> $OUT/results.txt
}
probe "plain nullable column        " "r.ss_sales_price"                                                       tpcds
probe "to_double() cast             " "to_double(r.ss_sales_price)"                                            tpcds
probe "CASE WHEN NULL -> 0.0        " "(CASE WHEN r.ss_sales_price IS NULL THEN 0.0 ELSE r.ss_sales_price END)" tpcds
probe "non-nullable control column  " "r.ss_sales_price"                                                       tpcdsnn
cat $OUT/results.txt
echo DONE > ~/Ameen/castwa.status; beat done
