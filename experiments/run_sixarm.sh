#!/usr/bin/env bash
# The paper's headline figure. Six arms, each answering one question a reader will ask:
#
#   1 stock-clone      git clone AsterixDB, undeclared column          -> the U-curve
#   2 stock-typefix    same, plus our auto-detected key, NO bucketing  -> does the key alone fix it?
#   3 stock-mixed      git clone, column holding SEVERAL types         -> what stock does with no key at all
#   4 bucket-plain     bucketing, no type fix                          -> bucketing's cost on its own
#   5 bucket-typefix   bucketing + auto key                            -> the two combined
#   6 bucket-best      bucketing + auto key + k-way + scaled buckets   -> everything on
#
# Arms are interleaved inside each round so cluster-restart drift (~12%) hits all six equally.
set -uo pipefail
cd "$(dirname "$0")"
export REPO_ROOT=~/Ameen/asterixdb JARDIR=~/Ameen/jars
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
CL=$REPO_ROOT/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT/opt/local
Q=http://127.0.0.1:19002/query/service
OUT=~/Ameen/sixarm; mkdir -p $OUT
ROUNDS=${ROUNDS:-3}; REPS=${REPS:-3}
MEMS=${MEMS:-"8MB 32MB 128MB 512MB 2048MB"}
NOBUCKET=2000000000

echo RUNNING > ~/Ameen/sixarm.status
beat(){ echo "$(date +%H:%M:%S) $1" > ~/Ameen/sixarm.heartbeat; }
fail(){ echo "$1" >&2; echo FAILED > ~/Ameen/sixarm.status; exit 1; }
dep(){ local tag=$1; shift
  ./deploy.sh "$@" > $OUT/deploy-$tag.log 2>&1 || fail "DEPLOY FAILED $tag"
  grep -q 'jar verified' $OUT/deploy-$tag.log || fail "jar not verified $tag"; }
snap(){ : > $OUT/.logsnap; for f in $CL/logs/nc-*.log; do
          [ -f "$f" ] && echo "$f $(wc -l < "$f")" >> $OUT/.logsnap; done; }
since(){ while read -r f n; do tail -n +$((n+1)) "$f"; done < $OUT/.logsnap; }
qt(){ curl -s -o /dev/null -m 3600 -w '%{time_total}' "$Q" --data-urlencode \
  "statement=USE $1; SET \`compiler.sortmemory\` \"$2\"; SELECT VALUE count(*) FROM (SELECT VALUE r FROM ds AS r ORDER BY r.k) AS x;"; }
cnt(){ curl -s -m 900 "$Q" --data-urlencode "statement=SELECT VALUE count(*) FROM $1.ds;" \
       | grep -o '"results":[^]]*' | grep -o '[0-9]\+' | tail -1; }

# Arm 3 needs a MULTI-TYPE column at the same scale as the others, or its number is not
# comparable. Build it once and reuse.
if [ "$(cnt mixbig 2>/dev/null || echo 0)" != "10000000" ]; then
  beat "load mixbig"
  echo "building 10M-row multi-type dataset..." | tee $OUT/load.txt
  PAD="pppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppp"
  curl -s -m 600 "$Q" --data-urlencode 'statement=DROP DATAVERSE mixbig IF EXISTS;' >/dev/null
  curl -s -m 600 "$Q" --data-urlencode 'statement=CREATE DATAVERSE mixbig; USE mixbig; CREATE TYPE t AS open {id:int64}; CREATE DATASET ds(t) PRIMARY KEY id;' >/dev/null
  for ((lo=1; lo<=10000000; lo+=1000000)); do
    hi=$((lo+999999)); beat "load mixbig $hi"
    curl -s -m 3600 -o /dev/null "$Q" --data-urlencode "statement=USE mixbig; INSERT INTO ds (
      SELECT VALUE {\"id\":x,
        \"k\": CASE WHEN x % 5 = 0 THEN double((x*48271) % 2000000)
                    WHEN x % 2 = 0 THEN (x*48271) % 2000000
                    ELSE string((x*48271) % 2000000) END,
        \"payload\":\"$PAD\"} FROM range($lo,$hi) AS x);"
  done
  N=$(cnt mixbig); echo "mixbig rows=$N" | tee -a $OUT/load.txt
  [ "$N" = "10000000" ] || fail "mixbig has $N rows, expected 10000000"
fi
for DV in test typed mixbig; do
  N=$(cnt $DV); echo "$DV rows=$N" | tee -a $OUT/rows.txt
  [ "$N" = "10000000" ] || fail "$DV has $N rows"
done

BASE="--broker none --heap 6g --cap-mult 1"
: > $OUT/times.txt; : > $OUT/keys.txt
for ROUND in $(seq 1 $ROUNDS); do
  for ARM in stock-clone stock-typefix stock-mixed bucket-plain bucket-typefix bucket-best; do
    beat "round$ROUND $ARM deploy"
    case $ARM in
      stock-clone)    dep "$ARM-r$ROUND" --jar $JARDIR/master.jar --broker stock --heap 6g;                  DV=test   ;;
      stock-mixed)    dep "$ARM-r$ROUND" --jar $JARDIR/master.jar --broker stock --heap 6g;                  DV=mixbig ;;
      stock-typefix)  dep "$ARM-r$ROUND" --jar $JARDIR/adaptive.jar $BASE --bucket-tuples $NOBUCKET \
                          --auto-type-key true;                                                              DV=test   ;;
      bucket-plain)   dep "$ARM-r$ROUND" --jar $JARDIR/adaptive.jar $BASE --auto-type-key false;              DV=test   ;;
      bucket-typefix) dep "$ARM-r$ROUND" --jar $JARDIR/adaptive.jar $BASE --auto-type-key true;               DV=test   ;;
      bucket-best)    dep "$ARM-r$ROUND" --jar $JARDIR/adaptive.jar $BASE --auto-type-key true \
                          --kway true --merge-fan-in 1000000;                                                DV=test   ;;
    esac
    for MEM in $MEMS; do
      beat "round$ROUND $ARM $MEM"; snap
      qt $DV $MEM >/dev/null
      for r in $(seq 1 $REPS); do echo "SA $ROUND $ARM $MEM $(qt $DV $MEM)" >> $OUT/times.txt; done
      since | grep -oE 'adaptive-sort-keys:.*|bucketTargetBytes=[0-9]+ runtimeDecisive=[a-z]+' \
        | sort -u | head -2 | sed "s/^/EV $ARM $MEM /" >> $OUT/keys.txt
    done
  done
  echo "round $ROUND done: $(wc -l < $OUT/times.txt) samples" | tee -a $OUT/progress.txt
done
echo DONE > ~/Ameen/sixarm.status; beat done
