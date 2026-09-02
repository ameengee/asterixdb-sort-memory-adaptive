#!/usr/bin/env bash
# Load ~7GB (80M rows) so the sort is genuinely I/O bound.
#
# Why this matters: at 859MB against ~38GB of page cache, no run ever reaches the device, so every
# number we have measures CPU only. That setup can show what bucketing COSTS but not what its
# overlap of sort work with arrival SAVES. With 80M rows (~6.9GB) and a ~48GB balloon, the cache
# left over (~4GB) is smaller than the data, so reads must hit the disk.
#
# Same row shape as `test` (open type, undeclared k) so results are comparable to everything else.
set -uo pipefail
cd "$(dirname "$0")"
Q=http://127.0.0.1:19002/query/service
OUT=~/Ameen/bigload; mkdir -p $OUT
ROWS=${ROWS:-80000000}; BATCH=${BATCH:-1000000}

echo RUNNING > ~/Ameen/bigload.status
beat(){ echo "$(date +%H:%M:%S) $1" > ~/Ameen/bigload.heartbeat; }
fail(){ echo "$1" >&2; echo FAILED > ~/Ameen/bigload.status; exit 1; }
cnt(){ curl -s -m 1800 "$Q" --data-urlencode "statement=SELECT VALUE count(*) FROM $1.ds;" \
       | grep -o '"results":[^]]*' | grep -o '[0-9]\+' | tail -1; }

HAVE=$(cnt big 2>/dev/null || echo 0)
if [ "$HAVE" = "$ROWS" ]; then echo "big already has $ROWS rows"; echo DONE > ~/Ameen/bigload.status; exit 0; fi

PAD="pppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppp"
beat "create"
curl -s -m 600 -o /dev/null "$Q" --data-urlencode 'statement=DROP DATAVERSE big IF EXISTS;'
curl -s -m 600 -o /dev/null "$Q" --data-urlencode \
  'statement=CREATE DATAVERSE big; USE big; CREATE TYPE t AS open {id:int64}; CREATE DATASET ds(t) PRIMARY KEY id;'

START=$(date +%s)
for ((lo=1; lo<=ROWS; lo+=BATCH)); do
  hi=$((lo+BATCH-1)); [ $hi -gt $ROWS ] && hi=$ROWS
  C=$(curl -s -m 3600 -o /dev/null -w '%{http_code}' "$Q" --data-urlencode \
    "statement=USE big; INSERT INTO ds (SELECT VALUE {\"id\":x,\"k\":(x*48271)%16000000,\"payload\":\"$PAD\"} FROM range($lo,$hi) AS x);")
  [ "$C" = 200 ] || fail "insert batch $lo-$hi failed with http $C"
  EL=$(( $(date +%s) - START )); PCT=$(( hi * 100 / ROWS ))
  beat "loaded $hi/$ROWS (${PCT}%), ${EL}s elapsed"
  echo "$(date +%H:%M:%S) loaded through $hi (${PCT}%), ${EL}s" >> $OUT/progress.txt
done
N=$(cnt big); echo "big rows=$N" | tee $OUT/rows.txt
[ "$N" = "$ROWS" ] || fail "big has $N rows, expected $ROWS"
echo DONE > ~/Ameen/bigload.status; beat "done: $N rows"
