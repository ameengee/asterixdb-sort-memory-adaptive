#!/usr/bin/env bash
# Generate real TPC-DS with DuckDB's dsdgen and load store_sales into AsterixDB.
#
# Why store_sales: it is the largest fact table, and -- critically -- the OFFICIAL AsterixDB TPC-DS
# schema declares nearly every column NULLABLE (bigint?/double?). A nullable column's type is a
# union, which the normalized-key provider has no case for, so it returns null and the sort runs
# with no normalized key at all. That is our nullable finding, on a real benchmark schema, not a
# synthetic one. Sorting on ss_sales_price (double?) or ss_customer_sk (bigint?) exercises it.
set -uo pipefail
SF=${SF:-10}
WORK=~/Ameen/tpcds; mkdir -p $WORK
Q=http://127.0.0.1:19002/query/service
DUCKDB=${DUCKDB:-/snap/bin/duckdb}
echo RUNNING > ~/Ameen/tpcds.status
beat(){ echo "$(date +%H:%M:%S) $1" > ~/Ameen/tpcds.heartbeat; }
fail(){ echo "$1" >&2; echo FAILED > ~/Ameen/tpcds.status; exit 1; }
sql(){ curl -s -m 7200 -o $WORK/.out -w '%{http_code}' "$Q" --data-urlencode "statement=$1"; }
cnt(){ curl -s -m 1800 "$Q" --data-urlencode "statement=SELECT VALUE count(*) FROM tpcds.store_sales;" \
       | grep -o '"results":[^]]*' | grep -o '[0-9]\+' | tail -1; }

# ---------- generate ----------
if [ ! -s $WORK/store_sales.json ]; then
  beat "dsdgen sf=$SF"
  # threads=2 keeps generation from saturating the box if anything else is measuring.
  $DUCKDB $WORK/tpcds.duckdb -c "
    SET threads=2;
    INSTALL tpcds; LOAD tpcds;
    CALL dsdgen(sf=$SF);
    COPY store_sales TO '$WORK/store_sales.json' (FORMAT JSON, ARRAY false);
  " > $WORK/dsdgen.log 2>&1 || fail "dsdgen failed: $(tail -5 $WORK/dsdgen.log)"
fi
[ -s $WORK/store_sales.json ] || fail "no store_sales.json produced"
ROWS_FILE=$(wc -l < $WORK/store_sales.json)
echo "generated $ROWS_FILE rows, $(du -sh $WORK/store_sales.json | cut -f1)" | tee $WORK/gen.txt

# ---------- load ----------
beat "create dataverse"
sql 'DROP DATAVERSE tpcds IF EXISTS;' >/dev/null
DDL='CREATE DATAVERSE tpcds; USE tpcds;
CREATE TYPE store_sales_type AS CLOSED {
  ss_sold_date_sk: bigint?, ss_sold_time_sk: bigint?, ss_item_sk: bigint,
  ss_customer_sk: bigint?, ss_cdemo_sk: bigint?, ss_hdemo_sk: bigint?, ss_addr_sk: bigint?,
  ss_store_sk: bigint?, ss_promo_sk: bigint?, ss_ticket_number: bigint, ss_quantity: bigint?,
  ss_wholesale_cost: double?, ss_list_price: double?, ss_sales_price: double?,
  ss_ext_discount_amt: double?, ss_ext_sales_price: double?, ss_ext_wholesale_cost: double?,
  ss_ext_list_price: double?, ss_ext_tax: double?, ss_coupon_amt: double?, ss_net_paid: double?,
  ss_net_paid_inc_tax: double?, ss_net_profit: double?
};
CREATE DATASET store_sales(store_sales_type) PRIMARY KEY ss_item_sk, ss_ticket_number;'
[ "$(sql "$DDL")" = 200 ] || fail "DDL failed: $(head -c 400 $WORK/.out)"

beat "loading"
C=$(sql "USE tpcds; LOAD DATASET store_sales USING localfs ((\"path\"=\"localhost://$WORK/store_sales.json\"),(\"format\"=\"json\"));")
[ "$C" = 200 ] || fail "LOAD failed ($C): $(head -c 500 $WORK/.out)"
N=$(cnt); echo "loaded rows=$N (file had $ROWS_FILE)" | tee -a $WORK/gen.txt
[ "$N" = "$ROWS_FILE" ] || fail "row mismatch: loaded $N, file $ROWS_FILE"
echo DONE > ~/Ameen/tpcds.status; beat "done: $N rows"
