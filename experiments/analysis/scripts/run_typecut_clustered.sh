#!/usr/bin/env bash
# Type-cut buckets seal on a TYPE CHANGE, so they can only form single-typed buckets when values of
# a type are CONTIGUOUS in scan order. `mixbig` alternates type essentially every tuple, which
# defeats the mechanism by construction -- no bucket is ever homogeneous, so none becomes exact.
#
# This builds the case where the mechanism CAN apply: the same three types, but clustered into
# contiguous id ranges. If type-cut helps anywhere, it helps here; if it does not help even here,
# the idea is dead rather than merely situational.
set -uo pipefail
cd "$(dirname "$0")"
EXP=$(cd ../.. && pwd)
export REPO_ROOT=~/Ameen/asterixdb JARDIR=~/Ameen/jars
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
CL=$REPO_ROOT/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT/opt/local
Q=http://127.0.0.1:19002/query/service
OUT=~/Ameen/typecut-clustered; mkdir -p $OUT
ROWS=${ROWS:-10000000}; MEMS=${MEMS:-"32MB 512MB 2048MB"}; ROUNDS=${ROUNDS:-2}; REPS=${REPS:-3}
echo RUNNING > ~/Ameen/typecutclust.status
beat(){ echo "$(date +%H:%M:%S) $1" > ~/Ameen/typecutclust.heartbeat; }
fail(){ echo "$1" >&2; echo FAILED > ~/Ameen/typecutclust.status; exit 1; }
dep(){ local tag=$1; shift
  (cd $EXP && ./deploy.sh "$@") > $OUT/deploy-$tag.log 2>&1 || fail "DEPLOY FAILED $tag"
  grep -q 'jar verified' $OUT/deploy-$tag.log || fail "jar not verified $tag"; }
qt(){ curl -s -o /dev/null -m 7200 -w '%{time_total}' "$Q" --data-urlencode \
  "statement=USE $1; SET \`compiler.sortmemory\` \"$2\"; SELECT VALUE count(*) FROM (SELECT VALUE r FROM ds AS r ORDER BY r.k) AS x;"; }
sql(){ curl -s -m 3600 -o /dev/null -w '%{http_code}' "$Q" --data-urlencode "statement=$1"; }
cnt(){ curl -s -m 900 "$Q" --data-urlencode "statement=SELECT VALUE count(*) FROM $1.ds;" \
       | grep -o '"results":[^]]*' | grep -o '[0-9]\+' | tail -1; }
snap(){ : > $OUT/.snap; for f in $CL/logs/nc-*.log; do [ -f "$f" ] && echo "$f $(wc -l < "$f")" >> $OUT/.snap; done; }
since(){ while read -r f n; do tail -n +$((n+1)) "$f"; done < $OUT/.snap; }

# ---- clustered dataset: three contiguous id ranges, one type each ----
if [ "$(cnt mixclust 2>/dev/null || echo 0)" != "$ROWS" ]; then
  beat "load mixclust"
  PAD="pppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppp"
  sql 'DROP DATAVERSE mixclust IF EXISTS;' >/dev/null
  [ "$(sql 'CREATE DATAVERSE mixclust; USE mixclust; CREATE TYPE t AS open {id:int64}; CREATE DATASET ds(t) PRIMARY KEY id;')" = 200 ] || fail "create failed"
  T1=$((ROWS/3)); T2=$((2*ROWS/3))
  for ((lo=1; lo<=ROWS; lo+=1000000)); do
    hi=$((lo+999999)); [ $hi -gt $ROWS ] && hi=$ROWS; beat "load mixclust $hi"
    C=$(sql "USE mixclust; INSERT INTO ds (SELECT VALUE {\"id\":x,
        \"k\": CASE WHEN x <= $T1 THEN (x*48271)%2000000
                    WHEN x <= $T2 THEN double((x*48271)%2000000)
                    ELSE string((x*48271)%2000000) END,
        \"payload\":\"$PAD\"} FROM range($lo,$hi) AS x);")
    [ "$C" = 200 ] || fail "insert $lo-$hi http $C"
  done
  N=$(cnt mixclust); echo "mixclust rows=$N" | tee $OUT/rows.txt
  [ "$N" = "$ROWS" ] || fail "mixclust has $N rows"
fi

: > $OUT/times.txt; : > $OUT/engagement.txt
for ROUND in $(seq 1 $ROUNDS); do
  for ARM in off on; do
    beat "round$ROUND typecut=$ARM"
    dep "tc-$ARM-r$ROUND" --jar $JARDIR/adaptive.jar --broker none --heap 6g --cap-mult 1 \
        --auto-type-key true --kway true --merge-fan-in 1000000 \
        --type-cut $([ $ARM = on ] && echo true || echo false)
    for MEM in $MEMS; do
      beat "round$ROUND typecut=$ARM @ $MEM"; snap
      qt mixclust $MEM > /dev/null
      for r in $(seq 1 $REPS); do echo "TCC $ROUND $ARM $MEM $(qt mixclust $MEM)" >> $OUT/times.txt; done
      since | grep -o 'runtimeDecisive=[a-z]*' | sort | uniq -c | sed "s/^/EV $ARM $MEM /" >> $OUT/engagement.txt
    done
  done
done
echo DONE > ~/Ameen/typecutclust.status; beat done
