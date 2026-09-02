#!/usr/bin/env bash
# The 13 (configuration x dataset) cells the 12 analysis graphs need, at one scale.
#
# Configurations:
#   A  stock jar, as cloned
#   B  auto-detected key, NO bucketing        ("stock + our type fix")
#   C  bucketing, NO type fix
#   D  bucketing + auto-detected key
#   E  bucketing + auto key + k-way           ("ideal settings")
#
# Datasets: <single>  one declared-able type, column UNDECLARED (open type)
#           <typed>   same data, column DECLARED int64
#           <multi>   the column genuinely holds int64 + double + string
#
# B and D are skipped on <typed>: when the column is declared, the provider hands back the native
# normalizer and our detector never engages, so those cells would duplicate A and C exactly.
#
# There is deliberately NO "user declares the type" cell for <multi>: AsterixDB cannot express a
# multi-type column (the DDL only builds unions via `?`), so that configuration does not exist.
# That absence is the finding, not a gap in the run.
set -uo pipefail
cd "$(dirname "$0")"
export REPO_ROOT=~/Ameen/asterixdb JARDIR=~/Ameen/jars
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
EXP=$REPO_ROOT/experiments
CL=$REPO_ROOT/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT/opt/local
Q=http://127.0.0.1:19002/query/service

SCALE=${SCALE:-small}
if [ "$SCALE" = small ]; then
  SINGLE=test; TYPED=typed; MULTI=mixbig
  MEMS=${MEMS:-"8MB 32MB 128MB 512MB 2048MB"}; ROUNDS=${ROUNDS:-2}; REPS=${REPS:-3}; EXPECT=10000000
else
  SINGLE=big;  TYPED=bigtyped; MULTI=bigmixed
  MEMS=${MEMS:-"32MB 512MB 2048MB"};          ROUNDS=${ROUNDS:-1}; REPS=${REPS:-2}; EXPECT=290000000
fi
OUT=~/Ameen/matrix-$SCALE; mkdir -p $OUT

echo RUNNING > ~/Ameen/matrix-$SCALE.status
beat(){ echo "$(date +%H:%M:%S) $1" > ~/Ameen/matrix-$SCALE.heartbeat; }
fail(){ echo "$1" >&2; echo FAILED > ~/Ameen/matrix-$SCALE.status; exit 1; }
dep(){ local tag=$1; shift
  (cd $EXP && ./deploy.sh "$@") > $OUT/deploy-$tag.log 2>&1 || fail "DEPLOY FAILED $tag"
  grep -q 'jar verified' $OUT/deploy-$tag.log || fail "jar not verified $tag"; }
qt(){ curl -s -o /dev/null -m 7200 -w '%{time_total}' "$Q" --data-urlencode \
  "statement=USE $1; SET \`compiler.sortmemory\` \"$2\"; SELECT VALUE count(*) FROM (SELECT VALUE r FROM ds AS r ORDER BY r.k) AS x;"; }
cnt(){ curl -s -m 1800 "$Q" --data-urlencode "statement=SELECT VALUE count(*) FROM $1.ds;" \
       | grep -o '"results":[^]]*' | grep -o '[0-9]\+' | tail -1; }
snap(){ : > $OUT/.snap; for f in $CL/logs/nc-*.log; do [ -f "$f" ] && echo "$f $(wc -l < "$f")" >> $OUT/.snap; done; }
since(){ while read -r f n; do tail -n +$((n+1)) "$f"; done < $OUT/.snap; }

for DV in $SINGLE $TYPED $MULTI; do
  N=$(cnt $DV); echo "$DV rows=$N" | tee -a $OUT/rows.txt
  [ "$N" = "$EXPECT" ] || fail "$DV has $N rows, expected $EXPECT"
done

BASE="--broker none --heap 6g --cap-mult 1"
NOBUCKET=2000000000
# cell = CONFIG:DATASET
CELLS="A:$SINGLE A:$TYPED A:$MULTI B:$SINGLE B:$MULTI C:$SINGLE C:$TYPED C:$MULTI D:$SINGLE D:$MULTI E:$SINGLE E:$TYPED E:$MULTI"

: > $OUT/times.txt; : > $OUT/config.txt
for ROUND in $(seq 1 $ROUNDS); do
  for CELL in $CELLS; do
    CFG=${CELL%%:*}; DV=${CELL##*:}
    beat "round$ROUND $CFG on $DV: deploy"
    case $CFG in
      A) dep "$CFG-$DV-r$ROUND" --jar $JARDIR/master.jar --broker stock --heap 6g ;;
      B) dep "$CFG-$DV-r$ROUND" --jar $JARDIR/adaptive.jar $BASE --auto-type-key true --bucket-tuples $NOBUCKET ;;
      C) dep "$CFG-$DV-r$ROUND" --jar $JARDIR/adaptive.jar $BASE --auto-type-key false ;;
      D) dep "$CFG-$DV-r$ROUND" --jar $JARDIR/adaptive.jar $BASE --auto-type-key true ;;
      E) dep "$CFG-$DV-r$ROUND" --jar $JARDIR/adaptive.jar $BASE --auto-type-key true --kway true --merge-fan-in 1000000 ;;
    esac
    for MEM in $MEMS; do
      beat "round$ROUND $CFG on $DV @ $MEM"; snap
      qt $DV $MEM >/dev/null      # warm the cell; discarded
      for r in $(seq 1 $REPS); do echo "MX $ROUND $CFG $DV $MEM $(qt $DV $MEM)" >> $OUT/times.txt; done
      since | grep -oE 'adaptive-sort-keys:.*|bucketTargetBytes=[0-9]+ runtimeDecisive=[a-z]+' \
        | sort -u | head -2 | sed "s/^/CFG $CFG $DV $MEM /" >> $OUT/config.txt
    done
  done
  echo "round $ROUND done: $(wc -l < $OUT/times.txt) samples" | tee -a $OUT/progress.txt
done
echo DONE > ~/Ameen/matrix-$SCALE.status; beat done
