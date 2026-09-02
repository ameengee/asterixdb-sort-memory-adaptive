#!/usr/bin/env bash
# The two bucketing claims that currently have NO evidence on the present build.
#
# PART A -- "good neighbor": bucketing interleaves sort work with data arrival instead of
#   load-everything-then-sort. Measured by spreadPct from the phase log: ~0 means all sorting
#   collapsed to one instant at the end; approaching 100 means it tracked arrival. Figure 1 showed
#   this, but it predates k-way merge, runtime decisiveness and bucket-count scaling.
#
# PART B -- shrink-side adaptivity: when a broker reclaims memory mid-sort, Stage 2 spills only the
#   SORTED PREFIX and keeps the unsorted tail, where stock must flush everything. Never measured
#   end-to-end. The comparison that isolates it is partialSpill true vs false under the SAME
#   reclamation schedule -- not ours-vs-stock, which would confound the key and bucketing effects.
set -uo pipefail
cd "$(dirname "$0")"
EXP=$(cd ../.. && pwd)
export REPO_ROOT=~/Ameen/asterixdb JARDIR=~/Ameen/jars
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
CL=$REPO_ROOT/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT/opt/local
Q=http://127.0.0.1:19002/query/service
OUT=~/Ameen/adaptivity; mkdir -p $OUT
MEMS=${MEMS:-"128MB 512MB"}; ROUNDS=${ROUNDS:-3}; REPS=${REPS:-3}
NOBUCKET=2000000000

echo RUNNING > ~/Ameen/adaptivity.status
beat(){ echo "$(date +%H:%M:%S) $1" > ~/Ameen/adaptivity.heartbeat; }
fail(){ echo "$1" >&2; echo FAILED > ~/Ameen/adaptivity.status; exit 1; }
dep(){ local tag=$1; shift
  (cd $EXP && ./deploy.sh "$@") > $OUT/deploy-$tag.log 2>&1 || fail "DEPLOY FAILED $tag"
  grep -q 'jar verified' $OUT/deploy-$tag.log || fail "jar not verified $tag"; }
qt(){ curl -s -o /dev/null -m 3600 -w '%{time_total}' "$Q" --data-urlencode \
  "statement=USE $1; SET \`compiler.sortmemory\` \"$2\"; SELECT VALUE count(*) FROM (SELECT VALUE r FROM ds AS r ORDER BY r.k) AS x;"; }
snap(){ : > $OUT/.snap; for f in $CL/logs/nc-*.log; do [ -f "$f" ] && echo "$f $(wc -l < "$f")" >> $OUT/.snap; done; }
since(){ while read -r f n; do tail -n +$((n+1)) "$f"; done < $OUT/.snap; }
COMMON="--heap 6g --cap-mult 1 --auto-type-key true --kway true --merge-fan-in 1000000"

# ================= PART A: neighbor property =================
: > $OUT/spread.txt
for ARM in bucketed flat; do
  beat "A $ARM"
  if [ $ARM = bucketed ]; then dep "A-$ARM" --jar $JARDIR/adaptive.jar --broker none $COMMON --phase-log true
  else dep "A-$ARM" --jar $JARDIR/adaptive.jar --broker none $COMMON --phase-log true --bucket-tuples $NOBUCKET; fi
  for MEM in $MEMS; do
    beat "A $ARM $MEM"; snap; qt test $MEM >/dev/null
    since | grep -o 'adaptive-sort-phase:.*' \
      | grep -oE 'spreadPct=[0-9]+|sortEvents=[0-9]+' | paste - - \
      | sed "s/^/SPREAD $ARM $MEM /" >> $OUT/spread.txt
  done
done

# ================= PART B: shrink under reclamation =================
: > $OUT/times.txt; : > $OUT/decisions.txt
for ROUND in $(seq 1 $ROUNDS); do
  for ARM in no-pressure reclaim-partial reclaim-full; do
    beat "B round$ROUND $ARM"
    case $ARM in
      no-pressure)     dep "B-$ARM-r$ROUND" --jar $JARDIR/adaptive.jar --broker none $COMMON ;;
      reclaim-partial) dep "B-$ARM-r$ROUND" --jar $JARDIR/adaptive.jar --broker periodic --period 10 \
                           --action reclaim --fraction 0.5 $COMMON --partial-spill true ;;
      reclaim-full)    dep "B-$ARM-r$ROUND" --jar $JARDIR/adaptive.jar --broker periodic --period 10 \
                           --action reclaim --fraction 0.5 $COMMON --partial-spill false ;;
    esac
    for MEM in $MEMS; do
      beat "B round$ROUND $ARM $MEM"; snap
      qt test $MEM >/dev/null
      for r in $(seq 1 $REPS); do echo "AD $ROUND $ARM $MEM $(qt test $MEM)" >> $OUT/times.txt; done
      # which reclamation paths actually fired -- proof the broker did something
      since | grep -o 'adaptive-sort: [a-z-]*' | sort | uniq -c \
        | sed "s/^/DEC $ARM $MEM /" >> $OUT/decisions.txt
    done
  done
  echo "round $ROUND: $(wc -l < $OUT/times.txt) samples" | tee -a $OUT/progress.txt
done
echo DONE > ~/Ameen/adaptivity.status; beat done
