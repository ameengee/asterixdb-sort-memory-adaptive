#!/usr/bin/env bash
# Does a NULLABLE declared column (k: int64?) get a normalized key in stock AsterixDB?
# The provider switches on the type tag and has no UNION case, so it should fall to
# `default:` and return null -- meaning a carefully-typed nullable column silently pays
# the same no-normalized-key penalty as an undeclared one. Verify, don't assume: the
# compiler may unwrap the union to its non-null branch before calling the provider.
#
# Evidence is the `adaptive-sort-keys:` line: nkcs=0 / nkTotalLen=0 == no key.
set -uo pipefail
cd "$(dirname "$0")"
export REPO_ROOT=~/Ameen/asterixdb JARDIR=~/Ameen/jars
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
CL=$REPO_ROOT/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT/opt/local
Q=http://127.0.0.1:19002/query/service
OUT=~/Ameen/nullable; mkdir -p $OUT
ROWS=${ROWS:-4000000}; REPS=${REPS:-5}
echo RUNNING > ~/Ameen/nullable.status
beat(){ echo "$(date +%H:%M:%S) $1" > ~/Ameen/nullable.heartbeat; }
fail(){ echo "$1" >&2; echo FAILED > ~/Ameen/nullable.status; exit 1; }
dep(){ local tag=$1; shift
  ./deploy.sh "$@" > $OUT/deploy-$tag.log 2>&1 || fail "DEPLOY FAILED $tag"
  grep -q 'jar verified' $OUT/deploy-$tag.log || fail "jar not verified $tag"; }
snap(){ : > $OUT/.logsnap; for f in $CL/logs/nc-*.log; do
          [ -f "$f" ] && echo "$f $(wc -l < "$f")" >> $OUT/.logsnap; done; }
since(){ while read -r f n; do tail -n +$((n+1)) "$f"; done < $OUT/.logsnap; }
sql(){ curl -s -m 7200 "$Q" --data-urlencode "statement=$1" -o $OUT/.sql.out -w '%{http_code}'; }
qt(){ curl -s -o /dev/null -m 3600 -w '%{time_total}' "$Q" --data-urlencode \
  "statement=USE $1; SET \`compiler.sortmemory\` \"$2\"; SELECT VALUE count(*) FROM (SELECT VALUE r FROM ds AS r ORDER BY r.k) AS x;"; }

# nn = NOT NULL (k: int64), nl = NULLABLE (k: int64?). Same data, same rows, only the
# declaration differs -- so any gap is attributable to the `?` alone.
beat load
PAD="pppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppp"
sql 'DROP DATAVERSE nn IF EXISTS;' >/dev/null
sql 'DROP DATAVERSE nl IF EXISTS;' >/dev/null
sql 'CREATE DATAVERSE nn; USE nn; CREATE TYPE t AS closed {id:int64,k:int64,payload:string}; CREATE DATASET ds(t) PRIMARY KEY id;' >/dev/null
sql 'CREATE DATAVERSE nl; USE nl; CREATE TYPE t AS closed {id:int64,k:int64?,payload:string}; CREATE DATASET ds(t) PRIMARY KEY id;' >/dev/null
B=1000000
for ((lo=1; lo<=ROWS; lo+=B)); do
  hi=$((lo+B-1)); [ $hi -gt $ROWS ] && hi=$ROWS; beat "load-$hi"
  for DV in nn nl; do
    C=$(sql "USE $DV; INSERT INTO ds (SELECT VALUE {\"id\":x,\"k\":(x*48271)%2000000,\"payload\":\"$PAD\"} FROM range($lo,$hi) AS x);")
    [ "$C" = 200 ] || fail "insert failed into $DV ($C): $(head -c 300 $OUT/.sql.out)"
  done
done
for DV in nn nl; do
  N=$(curl -s "$Q" --data-urlencode "statement=SELECT VALUE count(*) FROM $DV.ds;" | grep -o '[0-9]\+' | tail -1)
  echo "$DV rows=$N" | tee -a $OUT/rows.txt
  [ "$N" = "$ROWS" ] || fail "$DV has $N rows, expected $ROWS"
done

: > $OUT/times.txt; : > $OUT/keys.txt
for ROUND in 1 2 3; do
  for ARM in stock ours; do
    beat "r$ROUND $ARM deploy"
    if [ $ARM = stock ]; then dep "$ARM-r$ROUND" --jar $JARDIR/adaptive.jar --broker none --heap 6g \
                                  --kway true --merge-fan-in 1000000 --cap-mult 1
    else                      dep "$ARM-r$ROUND" --jar $JARDIR/adaptive.jar --broker none --heap 6g \
                                  --kway true --merge-fan-in 1000000 --cap-mult 1 --auto-type-key true; fi
    for DV in nn nl; do
      for MEM in 32MB 2048MB; do
        beat "r$ROUND $ARM $DV $MEM"; snap
        qt $DV $MEM >/dev/null
        for r in $(seq 1 $REPS); do echo "NB $ROUND $ARM $DV $MEM $(qt $DV $MEM)" >> $OUT/times.txt; done
        since | grep -o 'adaptive-sort-keys:.*' | head -1 | sed "s/^/KEYS $ARM $DV $MEM /" >> $OUT/keys.txt
      done
    done
  done
done
echo DONE > ~/Ameen/nullable.status; beat done
