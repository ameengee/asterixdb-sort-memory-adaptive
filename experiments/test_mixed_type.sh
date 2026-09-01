#!/usr/bin/env bash
# CORRECTNESS: a sort column holding MIXED types must still order exactly as stock does.
# The dynamic normalized key is only sound while the column is homogeneous; on a second distinct
# type tag it reports isKeyValid()==false and the sorter must discard the bucket ordering derived
# from those keys and re-sort with the comparator alone. This checks that the fallback actually
# produces stock's ordering, not merely "something sorted".
set -uo pipefail
cd "$(dirname "$0")"
export REPO_ROOT=~/Ameen/asterixdb JARDIR=~/Ameen/jars JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
Q=http://127.0.0.1:19002/query/service
CL=$REPO_ROOT/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT/opt/local
sql(){ curl -s -o /dev/null -m 600 "$Q" --data-urlencode "statement=$1"; }

echo "=== building a deliberately MIXED-TYPE column ==="
sql 'DROP DATAVERSE mixed IF EXISTS;'
sql 'CREATE DATAVERSE mixed; USE mixed; CREATE TYPE t AS open {id:int64}; CREATE DATASET ds(t) PRIMARY KEY id;'
# k is int64 for even ids, string for odd ids, double for every 5th -> three distinct type tags
sql 'USE mixed; INSERT INTO ds (SELECT VALUE {"id":x,
       "k": CASE WHEN x %% 5 = 0 THEN double((x*48271) %% 100000)
                 WHEN x %% 2 = 0 THEN (x*48271) %% 100000
                 ELSE string((x*48271) %% 100000) END }
     FROM range(1,200000) AS x);'
echo "  rows: $(curl -s "$Q" --data-urlencode 'statement=SELECT VALUE count(*) FROM mixed.ds;' | python3 -c 'import json,sys;print(json.load(sys.stdin)["results"][0])')"

run(){ # $1=label $2=extra deploy args
  ./deploy.sh --jar $2 >/dev/null 2>&1
  curl -s -o /tmp/mix-$1.json -m 900 "$Q" --data-urlencode \
    'statement=USE mixed; SET `compiler.sortmemory` "4MB"; SELECT VALUE r FROM ds AS r ORDER BY r.k;'
  echo "  $1: $(python3 -c "
import json;d=json.load(open('/tmp/mix-$1.json'));r=d.get('results',[])
print(f'rows={len(r)} status={d.get(\"status\")}')")"
}
echo "=== stock ordering (the reference) ==="
run stock "$JARDIR/master.jar --broker stock --heap 6g"
echo "=== ours, auto-detect ON (must invalidate and fall back) ==="
run ours "$JARDIR/adaptive.jar --broker none --heap 6g --kway true --cap-mult 1 --auto-type-key true"
echo "=== did the sorter report invalidation? ==="
grep -ho 'normalized keys invalidated.*' $CL/logs/nc-*.log | tail -2 || echo "  (no invalidation logged)"
echo "=== DO THE TWO ORDERINGS MATCH EXACTLY? ==="
python3 - <<'PY'
import json
a=json.load(open('/tmp/mix-stock.json')).get('results',[])
b=json.load(open('/tmp/mix-ours.json')).get('results',[])
print(f"  stock rows={len(a)}  ours rows={len(b)}")
if not a or not b: print("  FAIL: empty result"); raise SystemExit(1)
ka=[x.get('k') for x in a]; kb=[x.get('k') for x in b]
same = ka==kb
print(f"  key sequences identical: {same}")
if not same:
    for i,(x,y) in enumerate(zip(ka,kb)):
        if x!=y: print(f"  first divergence at {i}: stock={x!r} ours={y!r}"); break
ids_a=sorted(x['id'] for x in a); ids_b=sorted(x['id'] for x in b)
print(f"  same row set (no loss/dupes): {ids_a==ids_b}")
PY
