#!/usr/bin/env bash
# Tests every U-curve theory in one pass. Writes to ~/Ameen/theories/ with a status + heartbeat
# file so an external monitor can tell what is happening and when it is done.
#
#  T1  no normalized key   : untyped (open, k undeclared) vs typed (closed, k: int64)
#  T2  string keys         : typed k_str (1-int prefix, INDECISIVE) vs typed k (2-int, DECISIVE)
#  T3  N*log2(runSize)     : instrumented stage split + run count across memory levels
#  T4  bucket size         : sweep bucketTargetTuples, measure moves/tuple and time
#  T5  GC                  : gc log across memory levels
set -uo pipefail
cd "$(dirname "$0")"
export REPO_ROOT=~/Ameen/asterixdb JARDIR=~/Ameen/jars
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
CL=$REPO_ROOT/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT/opt/local
Q=http://127.0.0.1:19002/query/service
OUT=~/Ameen/theories; mkdir -p $OUT
ROWS=${ROWS:-10000000}; REPS=${REPS:-5}
echo RUNNING > ~/Ameen/theories.status
beat(){ echo "$1" > ~/Ameen/theories.heartbeat; }
fail(){ echo "$1" >&2; echo FAILED > ~/Ameen/theories.status; exit 1; }
dep(){ local tag=$1; shift
  ./deploy.sh "$@" > $OUT/deploy-$tag.log 2>&1 || fail "DEPLOY FAILED $tag"
  grep -q 'jar verified' $OUT/deploy-$tag.log || fail "jar not verified $tag"; }
# $1=dataverse $2=key $3=sortmemory
qt(){ curl -s -o /dev/null -m 3600 -w '%{time_total}' "$Q" --data-urlencode \
  "statement=USE $1; SET \`compiler.sortmemory\` \"$3\"; SELECT VALUE count(*) FROM (SELECT VALUE r FROM ds AS r ORDER BY r.$2) AS x;"; }
sql(){ curl -s -o /dev/null -m 7200 "$Q" --data-urlencode "statement=$1"; }

# ---------- data ----------
beat "load"
echo "=== loading ${ROWS} rows into untyped test.ds and typed typed.ds ===" | tee $OUT/load.txt
PAD="pppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppp"
sql 'DROP DATAVERSE test IF EXISTS;'
sql 'CREATE DATAVERSE test; USE test; CREATE TYPE t AS open {id:int64}; CREATE DATASET ds(t) PRIMARY KEY id;'
sql 'DROP DATAVERSE typed IF EXISTS;'
sql 'CREATE DATAVERSE typed; USE typed; CREATE TYPE t AS closed {id:int64,k:int64,k_str:string,payload:string}; CREATE DATASET ds(t) PRIMARY KEY id;'
sql 'DROP DATAVERSE small IF EXISTS;'
sql 'CREATE DATAVERSE small; USE small; CREATE TYPE t AS open {id:int64}; CREATE DATASET ds(t) PRIMARY KEY id;'
sql "USE small; INSERT INTO ds (SELECT VALUE {\"id\":x,\"k\":(x*48271)%700000,\"payload\":\"$PAD\"} FROM range(1,400000) AS x);"
B=1000000
for ((lo=1; lo<=ROWS; lo+=B)); do
  hi=$((lo+B-1)); [ $hi -gt $ROWS ] && hi=$ROWS
  beat "load-$hi"
  sql "USE test;  INSERT INTO ds (SELECT VALUE {\"id\":x,\"k\":(x*48271)%2000000,\"k_str\":string((x*48271)%2000000)||\"-key\",\"payload\":\"$PAD\"} FROM range($lo,$hi) AS x);"
  sql "USE typed; INSERT INTO ds (SELECT VALUE {\"id\":x,\"k\":(x*48271)%2000000,\"k_str\":string((x*48271)%2000000)||\"-key\",\"payload\":\"$PAD\"} FROM range($lo,$hi) AS x);"
  echo "  loaded through $hi" | tee -a $OUT/load.txt
done

# ---------- T1 + T2: key handling, across memory ----------
echo "=== T1/T2: key handling x memory ===" | tee $OUT/keys.txt
for ARM in stock kway; do
  for MEM in 32MB 320MB 2048MB; do
    beat "T12-$ARM-$MEM"
    if [ $ARM = stock ]; then dep "T12-$ARM-$MEM" --jar $JARDIR/master.jar --broker stock --heap 6g
    else dep "T12-$ARM-$MEM" --jar $JARDIR/adaptive.jar --broker none --heap 6g --kway true \
             --merge-fan-in 1000000 --cap-mult 1; fi
    for CFG in "test:k:untyped-bigint-NOKEY" "typed:k:typed-bigint-DECISIVE" "typed:k_str:typed-string-INDECISIVE"; do
      dv=${CFG%%:*}; rest=${CFG#*:}; key=${rest%%:*}; lbl=${rest#*:}
      qt $dv $key $MEM >/dev/null; qt $dv $key $MEM >/dev/null   # 2 warmups
      for r in $(seq 1 $REPS); do echo "KEYS $ARM $MEM $lbl $(qt $dv $key $MEM)" >> $OUT/keys.txt; done
    done
  done
done

# ---------- T3: stage split + run count vs memory (instrumented) ----------
echo "=== T3: run structure vs memory ===" | tee $OUT/t3.txt
for MEM in 8MB 32MB 128MB 320MB 1024MB 2048MB; do
  for DV in test typed; do
    beat "T3-$MEM-$DV"
    dep "T3-$MEM-$DV" --jar $JARDIR/adaptive.jar --broker none --heap 6g --kway true \
        --merge-fan-in 1000000 --cap-mult 1 --phase-log true
    qt $DV k $MEM >/dev/null; qt $DV k $MEM >/dev/null
    S=$(cat $CL/logs/nc-*.log | wc -l); T=$(qt $DV k $MEM)
    echo "T3 $DV $MEM time=$T" >> $OUT/t3.txt
    cat $CL/logs/nc-*.log | tail -n +$((S+1)) | grep -o 'adaptive-sort-phase:.*' | sed "s/^/T3PHASE $DV $MEM /" >> $OUT/t3.txt
  done
done

# ---------- T4: bucket size sweep ----------
echo "=== T4: bucket size (tuples) ===" | tee $OUT/t4.txt
for BT in 256 1024 4096 16384 65536 0; do
  for DV in test typed; do
    beat "T4-$BT-$DV"
    dep "T4-$BT-$DV" --jar $JARDIR/adaptive.jar --broker none --heap 6g --kway true \
        --merge-fan-in 1000000 --cap-mult 1 --bucket-tuples $BT --phase-log true
    qt $DV k 320MB >/dev/null; qt $DV k 320MB >/dev/null
    S=$(cat $CL/logs/nc-*.log | wc -l)
    for r in $(seq 1 $REPS); do echo "T4 $DV bt=$BT $(qt $DV k 320MB)" >> $OUT/t4.txt; done
    cat $CL/logs/nc-*.log | tail -n +$((S+1)) | grep -o 'adaptive-sort-phase:.*' | head -2 \
      | sed "s/^/T4PHASE $DV bt=$BT /" >> $OUT/t4.txt
  done
done

beat done; echo DONE > ~/Ameen/theories.status; echo THEORIES_COMPLETE
