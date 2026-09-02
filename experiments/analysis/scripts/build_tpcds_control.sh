#!/usr/bin/env bash
# CORRECTED non-nullable control for the TPC-DS comparison.
#
# The first attempt projected only 3 of the 23 columns, so it sorted far narrower rows and the
# measured gap conflated "no normalized key" with "much smaller payload" -- it read as 2.46-3.21x
# when a large part of that was row width. This keeps ALL 23 columns and changes exactly one thing:
# the declarations drop `?` and nulls are coalesced. Row count, column count and payload width are
# then identical, so any remaining difference is attributable to the declaration alone.
set -uo pipefail
Q=http://127.0.0.1:19002/query/service
OUT=~/Ameen/tpcdsnn2; mkdir -p $OUT
echo RUNNING > ~/Ameen/tpcdsnn2.status
fail(){ echo "$1" >&2; echo FAILED > ~/Ameen/tpcdsnn2.status; exit 1; }
sql(){ curl -s -m 7200 -o $OUT/.out -w '%{http_code}' "$Q" --data-urlencode "statement=$1"; }
cnt(){ curl -s -m 1800 "$Q" --data-urlencode "statement=SELECT VALUE count(*) FROM $1.store_sales;" \
       | grep -o '"results":[^]]*' | grep -o '[0-9]\+' | tail -1; }
N0=$(cnt tpcds); [ "$N0" -gt 1000000 ] || fail "tpcds missing ($N0)"
if [ "$(cnt tpcdsnn2 2>/dev/null || echo 0)" != "$N0" ]; then
  sql 'DROP DATAVERSE tpcdsnn2 IF EXISTS;' >/dev/null
  [ "$(sql 'CREATE DATAVERSE tpcdsnn2; USE tpcdsnn2;
    CREATE TYPE t AS CLOSED {
  ss_sold_date_sk: bigint,
  ss_sold_time_sk: bigint,
  ss_item_sk: bigint,
  ss_customer_sk: bigint,
  ss_cdemo_sk: bigint,
  ss_hdemo_sk: bigint,
  ss_addr_sk: bigint,
  ss_store_sk: bigint,
  ss_promo_sk: bigint,
  ss_ticket_number: bigint,
  ss_quantity: bigint,
  ss_wholesale_cost: double,
  ss_list_price: double,
  ss_sales_price: double,
  ss_ext_discount_amt: double,
  ss_ext_sales_price: double,
  ss_ext_wholesale_cost: double,
  ss_ext_list_price: double,
  ss_ext_tax: double,
  ss_coupon_amt: double,
  ss_net_paid: double,
  ss_net_paid_inc_tax: double,
  ss_net_profit: double
    };
    CREATE DATASET store_sales(t) PRIMARY KEY ss_item_sk, ss_ticket_number;')" = 200 ] \
    || fail "DDL failed: $(head -c 400 $OUT/.out)"
  C=$(sql 'USE tpcdsnn2; INSERT INTO store_sales (SELECT (CASE WHEN r.ss_sold_date_sk IS NULL THEN 0 ELSE r.ss_sold_date_sk END) AS ss_sold_date_sk,
         (CASE WHEN r.ss_sold_time_sk IS NULL THEN 0 ELSE r.ss_sold_time_sk END) AS ss_sold_time_sk,
         (CASE WHEN r.ss_item_sk IS NULL THEN 0 ELSE r.ss_item_sk END) AS ss_item_sk,
         (CASE WHEN r.ss_customer_sk IS NULL THEN 0 ELSE r.ss_customer_sk END) AS ss_customer_sk,
         (CASE WHEN r.ss_cdemo_sk IS NULL THEN 0 ELSE r.ss_cdemo_sk END) AS ss_cdemo_sk,
         (CASE WHEN r.ss_hdemo_sk IS NULL THEN 0 ELSE r.ss_hdemo_sk END) AS ss_hdemo_sk,
         (CASE WHEN r.ss_addr_sk IS NULL THEN 0 ELSE r.ss_addr_sk END) AS ss_addr_sk,
         (CASE WHEN r.ss_store_sk IS NULL THEN 0 ELSE r.ss_store_sk END) AS ss_store_sk,
         (CASE WHEN r.ss_promo_sk IS NULL THEN 0 ELSE r.ss_promo_sk END) AS ss_promo_sk,
         (CASE WHEN r.ss_ticket_number IS NULL THEN 0 ELSE r.ss_ticket_number END) AS ss_ticket_number,
         (CASE WHEN r.ss_quantity IS NULL THEN 0 ELSE r.ss_quantity END) AS ss_quantity,
         (CASE WHEN r.ss_wholesale_cost IS NULL THEN 0.0 ELSE r.ss_wholesale_cost END) AS ss_wholesale_cost,
         (CASE WHEN r.ss_list_price IS NULL THEN 0.0 ELSE r.ss_list_price END) AS ss_list_price,
         (CASE WHEN r.ss_sales_price IS NULL THEN 0.0 ELSE r.ss_sales_price END) AS ss_sales_price,
         (CASE WHEN r.ss_ext_discount_amt IS NULL THEN 0.0 ELSE r.ss_ext_discount_amt END) AS ss_ext_discount_amt,
         (CASE WHEN r.ss_ext_sales_price IS NULL THEN 0.0 ELSE r.ss_ext_sales_price END) AS ss_ext_sales_price,
         (CASE WHEN r.ss_ext_wholesale_cost IS NULL THEN 0.0 ELSE r.ss_ext_wholesale_cost END) AS ss_ext_wholesale_cost,
         (CASE WHEN r.ss_ext_list_price IS NULL THEN 0.0 ELSE r.ss_ext_list_price END) AS ss_ext_list_price,
         (CASE WHEN r.ss_ext_tax IS NULL THEN 0.0 ELSE r.ss_ext_tax END) AS ss_ext_tax,
         (CASE WHEN r.ss_coupon_amt IS NULL THEN 0.0 ELSE r.ss_coupon_amt END) AS ss_coupon_amt,
         (CASE WHEN r.ss_net_paid IS NULL THEN 0.0 ELSE r.ss_net_paid END) AS ss_net_paid,
         (CASE WHEN r.ss_net_paid_inc_tax IS NULL THEN 0.0 ELSE r.ss_net_paid_inc_tax END) AS ss_net_paid_inc_tax,
         (CASE WHEN r.ss_net_profit IS NULL THEN 0.0 ELSE r.ss_net_profit END) AS ss_net_profit
           FROM tpcds.store_sales AS r);')
  [ "$C" = 200 ] || fail "INSERT failed ($C): $(head -c 400 $OUT/.out)"
fi
N1=$(cnt tpcdsnn2); echo "tpcdsnn2 rows=$N1 (tpcds=$N0)" | tee $OUT/rows.txt
[ "$N1" = "$N0" ] || fail "row mismatch $N1 vs $N0"
echo DONE > ~/Ameen/tpcdsnn2.status
