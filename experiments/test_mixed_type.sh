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
# Report failures instead of swallowing them: an earlier version hid a broken INSERT behind
# -o /dev/null and produced a vacuous "0 rows, all good" result.
sql(){ local out
  out=$(curl -s -m 600 "$Q" --data-urlencode "statement=$1")
  if ! echo "$out" | grep -q '"status": *"success"'; then
    echo "SQL FAILED: $(echo "$out" | python3 -c 'import json,sys; print(str(json.load(sys.stdin).get("errors"))[:300])' 2>/dev/null || echo "$out" | head -c 200)" >&2
    return 1
  fi; }

echo "=== building a deliberately MIXED-TYPE column ==="
sql 'DROP DATAVERSE mixed IF EXISTS;'
sql 'CREATE DATAVERSE mixed; USE mixed; CREATE TYPE t AS open {id:int64}; CREATE DATASET ds(t) PRIMARY KEY id;'
# k is int64 for even ids, string for odd ids, double for every 5th -> three distinct type tags.
# NOTE: those three are all encodable by the ordering class, so this case tests CROSS-TYPE
# ORDERING and deliberately does NOT invalidate. The `hex` rows below are what force invalidation:
# BINARY's tag falls between the numeric tags, so it cannot be ordered against the merged numeric
# class and the computer must refuse. Keep both cases -- they cover different code paths.
sql 'USE mixed; INSERT INTO ds (SELECT VALUE {"id":x,
       "k": CASE WHEN x % 5 = 0 THEN double((x*48271) % 100000)
                 WHEN x % 2 = 0 THEN (x*48271) % 100000
                 ELSE string((x*48271) % 100000) END }
     FROM range(1,200000) AS x);' || exit 1
ROWS=$(curl -s "$Q" --data-urlencode 'statement=SELECT VALUE count(*) FROM mixed.ds;' | python3 -c 'import json,sys;print(json.load(sys.stdin)["results"][0])')
echo "  rows: $ROWS"
[ "$ROWS" -gt 0 ] || { echo "ABORT: dataset is empty, nothing to test" >&2; exit 1; }

DV=${DV:-mixed}   # which dataverse the run/compare below queries

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
echo "=== ours, auto-detect ON ==="
: > /tmp/mix.logsnap
for f in $CL/logs/nc-*.log; do [ -f "$f" ] && echo "$f $(wc -l < "$f")" >> /tmp/mix.logsnap; done
run ours "$JARDIR/adaptive.jar --broker none --heap 6g --kway true --cap-mult 1 --auto-type-key true"
echo "=== did the sorter report invalidation on THIS run? ==="
# Per-file offsets: tailing a re-concatenation of several growing logs re-reads old content.
since_snap(){ while read -r f n; do tail -n +$((n+1)) "$f"; done < /tmp/mix.logsnap; }
since_snap | grep -o 'normalized keys invalidated.*' | tail -2 || echo "  (no invalidation logged)"
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


# ---------------------------------------------------------------------------------------------
# CASE 2: force the INVALIDATION path. hex() yields BINARY, whose raw tag (9) falls between the
# numeric tags, so it cannot sit correctly relative to the merged numeric class. The computer must
# mark the key invalid and the sorter must re-derive ordering from the comparator alone -- and the
# result must STILL match stock exactly. Without this case the fallback is never executed.
echo
echo "=== CASE 2: column containing BINARY (must invalidate) ==="
sql 'DROP DATAVERSE mixed2 IF EXISTS;'
sql 'CREATE DATAVERSE mixed2; USE mixed2; CREATE TYPE t AS open {id:int64}; CREATE DATASET ds(t) PRIMARY KEY id;'
sql 'USE mixed2; INSERT INTO ds (SELECT VALUE {"id":x,
       "k": CASE WHEN x % 97 = 0 THEN hex(string((x*48271) % 100000))
                 WHEN x % 5 = 0  THEN double((x*48271) % 100000)
                 WHEN x % 2 = 0  THEN (x*48271) % 100000
                 ELSE string((x*48271) % 100000) END }
     FROM range(1,200000) AS x);' || exit 1
export DV=mixed2
run stock "$JARDIR/master.jar --broker stock --heap 6g"
: > /tmp/mix.logsnap
for f in $CL/logs/nc-*.log; do [ -f "$f" ] && echo "$f $(wc -l < "$f")" >> /tmp/mix.logsnap; done
run ours "$JARDIR/adaptive.jar --broker none --heap 6g --kway true --cap-mult 1 --auto-type-key true"
echo "--- invalidation MUST appear here ---"
if since_snap | grep -qE 'normalized key abandoned|normalized keys invalidated'; then
  echo "  OK: fallback fired"
else
  echo "  FAIL: expected invalidation on a BINARY-containing column, none logged" >&2
fi
python3 - <<'PY2'
import json
a=json.load(open('/tmp/mix-stock.json')).get('results',[])
b=json.load(open('/tmp/mix-ours.json')).get('results',[])
print(f"  stock rows={len(a)}  ours rows={len(b)}")
print(f"  key sequences identical: {a==b}")
print(f"  same row set (no loss/dupes): {sorted(map(str,a))==sorted(map(str,b))}")
PY2
