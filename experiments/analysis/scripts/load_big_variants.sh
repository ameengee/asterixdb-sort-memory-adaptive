#!/usr/bin/env bash
# Graphs 7-12 need all THREE dataset shapes at 7GB scale; only the untyped one (`big`) exists.
#   bigtyped  closed type, k declared int64  -> "user declares the type"
#   bigmixed  open type, k holds int64/double/string -> the multi-type case
# Same row count and generator as `big` so the three are comparable.
set -uo pipefail
Q=http://127.0.0.1:19002/query/service
ROWS=${ROWS:-290000000}; BATCH=${BATCH:-1000000}
OUT=~/Ameen/bigvariants; mkdir -p $OUT
echo RUNNING > ~/Ameen/bigvariants.status
beat(){ echo "$(date +%H:%M:%S) $1" > ~/Ameen/bigvariants.heartbeat; }
fail(){ echo "$1" >&2; echo FAILED > ~/Ameen/bigvariants.status; exit 1; }
sql(){ curl -s -m 3600 -o /dev/null -w '%{http_code}' "$Q" --data-urlencode "statement=$1"; }
cnt(){ curl -s -m 1800 "$Q" --data-urlencode "statement=SELECT VALUE count(*) FROM $1.ds;" \
       | grep -o '"results":[^]]*' | grep -o '[0-9]\+' | tail -1; }
PAD="pppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppp"

load(){ # $1=dataverse $2=create-ddl $3=value-expression-for-k
  local DV=$1 DDL=$2 KEXPR=$3
  if [ "$(cnt $DV 2>/dev/null || echo 0)" = "$ROWS" ]; then echo "$DV already loaded"; return; fi
  beat "create $DV"
  sql "DROP DATAVERSE $DV IF EXISTS;" >/dev/null
  [ "$(sql "$DDL")" = 200 ] || fail "create failed for $DV"
  local START=$(date +%s)
  for ((lo=1; lo<=ROWS; lo+=BATCH)); do
    local hi=$((lo+BATCH-1)); [ $hi -gt $ROWS ] && hi=$ROWS
    local C=$(sql "USE $DV; INSERT INTO ds (SELECT VALUE {\"id\":x,\"k\":$KEXPR,\"payload\":\"$PAD\"} FROM range($lo,$hi) AS x);")
    [ "$C" = 200 ] || fail "$DV insert $lo-$hi http $C"
    beat "$DV $hi/$ROWS ($((hi*100/ROWS))%), $(( $(date +%s) - START ))s"
  done
  local N=$(cnt $DV); echo "$DV rows=$N" | tee -a $OUT/rows.txt
  [ "$N" = "$ROWS" ] || fail "$DV has $N rows, expected $ROWS"
}

load bigtyped \
  'CREATE DATAVERSE bigtyped; USE bigtyped; CREATE TYPE t AS closed {id:int64,k:int64,payload:string}; CREATE DATASET ds(t) PRIMARY KEY id;' \
  '(x*48271)%16000000'

load bigmixed \
  'CREATE DATAVERSE bigmixed; USE bigmixed; CREATE TYPE t AS open {id:int64}; CREATE DATASET ds(t) PRIMARY KEY id;' \
  'CASE WHEN x % 5 = 0 THEN double((x*48271)%16000000) WHEN x % 2 = 0 THEN (x*48271)%16000000 ELSE string((x*48271)%16000000) END'

echo DONE > ~/Ameen/bigvariants.status; beat done
