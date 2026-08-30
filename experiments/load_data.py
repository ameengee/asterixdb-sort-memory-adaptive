#!/usr/bin/env python3
"""
Memory-adaptive sort -- dataset loader.

Builds sort workloads big enough that queries last long enough to span a memory change (see
paper_experiment_plan.md section 1 -- the governing ratio is query_duration / memory_change_period).

Two modes:

  synthetic   Generated in-database via range(); no download, no external files. Rows carry
              sort keys of SEVERAL TYPES so experiments can vary ptrSize, which ranges 3->7 ints
              and changes the real cache footprint of a "256KB bucket":
                k_int    INTEGER  -> 1 int  (decisive)
                k_big    BIGINT   -> 2 ints (decisive)
                k_dbl    DOUBLE   -> 2 ints (decisive)
                k_str    STRING   -> 1 int  (INDECISIVE: falls through to the comparator)
              Scale with --rows.

  tpch        Loads TPC-H lineitem from local .tbl files produced by dbgen. Point --tpch-dir at
              the directory holding lineitem.tbl. Sort columns of interest:
                l_orderkey (BIGINT), l_quantity (DOUBLE), l_shipdate (DATE -> 1 int),
                l_comment (STRING, indecisive)

Usage:
  ./load_data.py --mode synthetic --rows 5000000
  ./load_data.py --mode synthetic --rows 20000000 --batch 1000000
  ./load_data.py --mode tpch --tpch-dir /data/tpch-sf1
  ./load_data.py --mode synthetic --rows 5000000 --drop      # start clean
"""

import argparse
import json
import sys
import time
import urllib.parse
import urllib.request

DEFAULT_ENDPOINT = "http://127.0.0.1:19002/query/service"


def q(endpoint, statement, timeout=7200):
    data = urllib.parse.urlencode({"statement": statement}).encode()
    with urllib.request.urlopen(endpoint, data=data, timeout=timeout) as resp:
        payload = json.load(resp)
    if payload.get("status") != "success":
        raise RuntimeError(json.dumps(payload.get("errors", payload))[:800])
    return payload


def load_synthetic(args):
    ep = args.endpoint
    if args.drop:
        print("[load] dropping dataverse", args.dataverse)
        q(ep, f"DROP DATAVERSE {args.dataverse} IF EXISTS;")

    print(f"[load] creating {args.dataverse}.{args.dataset}")
    q(ep, f"""
        CREATE DATAVERSE {args.dataverse} IF NOT EXISTS;
        USE {args.dataverse};
        CREATE TYPE t IF NOT EXISTS AS open {{ id: int64 }};
        CREATE DATASET {args.dataset}(t) IF NOT EXISTS PRIMARY KEY id;
    """)

    pad = "x" * args.pad_bytes
    total = args.rows
    batch = args.batch
    done = 0
    started = time.time()
    while done < total:
        lo = done + 1
        hi = min(done + batch, total)
        # Keys are deliberately NOT correlated with id, so the sort cannot be skipped and the
        # input is in no useful pre-existing order.
        stmt = f"""
            USE {args.dataverse};
            INSERT INTO {args.dataset} (
              SELECT VALUE {{
                "id":    x,
                "k":     (x * 48271) % 2000000,
                "k_int": (x * 48271) % 2000000,
                "k_big": (x * 2654435761) % 9000000000,
                "k_dbl": ((x * 48271) % 2000000) / 7.0,
                "k_str": string(  (x * 48271) % 2000000 ) || "-key",
                "payload": "{pad}"
              }}
              FROM range({lo}, {hi}) AS x
            );
        """
        t0 = time.time()
        q(ep, stmt)
        done = hi
        rate = done / max(1e-9, time.time() - started)
        print(f"[load] {done:,}/{total:,} rows  (+{hi - lo + 1:,} in {time.time() - t0:.1f}s, "
              f"{rate:,.0f} rows/s)", flush=True)

    n = q(ep, f"SELECT VALUE count(*) FROM {args.dataverse}.{args.dataset};")["results"][0]
    print(f"[load] done: {n:,} rows in {time.time() - started:.1f}s")
    if n != total:
        print(f"[load] WARNING: expected {total:,} rows, found {n:,}", file=sys.stderr)


def load_tpch(args):
    ep = args.endpoint
    if not args.tpch_dir:
        sys.exit("--tpch-dir is required for --mode tpch")
    if args.drop:
        q(ep, f"DROP DATAVERSE {args.dataverse} IF EXISTS;")

    print("[load] creating TPC-H lineitem schema")
    q(ep, f"""
        CREATE DATAVERSE {args.dataverse} IF NOT EXISTS;
        USE {args.dataverse};
        CREATE TYPE LineItemType IF NOT EXISTS AS closed {{
          l_orderkey: bigint, l_partkey: bigint, l_suppkey: bigint, l_linenumber: bigint,
          l_quantity: double, l_extendedprice: double, l_discount: double, l_tax: double,
          l_returnflag: string, l_linestatus: string, l_shipdate: string,
          l_commitdate: string, l_receiptdate: string, l_shipinstruct: string,
          l_shipmode: string, l_comment: string
        }};
        CREATE DATASET lineitem(LineItemType) IF NOT EXISTS
          PRIMARY KEY l_orderkey, l_linenumber;
    """)

    path = args.tpch_dir.rstrip("/") + "/lineitem.tbl"
    print(f"[load] loading {path} (delimiter '|')")
    started = time.time()
    q(ep, f"""
        USE {args.dataverse};
        LOAD DATASET lineitem USING localfs
          (("path"="127.0.0.1://{path}"), ("format"="delimited-text"), ("delimiter"="|"));
    """)
    n = q(ep, f"SELECT VALUE count(*) FROM {args.dataverse}.lineitem;")["results"][0]
    print(f"[load] done: {n:,} rows in {time.time() - started:.1f}s")


def main():
    ap = argparse.ArgumentParser(description="Load sort-experiment datasets")
    ap.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    ap.add_argument("--mode", choices=["synthetic", "tpch"], default="synthetic")
    ap.add_argument("--dataverse", default="test")
    ap.add_argument("--dataset", default="ds")
    ap.add_argument("--rows", type=int, default=5_000_000)
    ap.add_argument("--batch", type=int, default=500_000,
                    help="rows per INSERT; smaller uses less memory during load")
    ap.add_argument("--pad-bytes", type=int, default=100,
                    help="payload width; widens rows so sorts spill sooner")
    ap.add_argument("--tpch-dir", default=None)
    ap.add_argument("--drop", action="store_true", help="drop the dataverse first")
    args = ap.parse_args()

    if args.mode == "synthetic":
        load_synthetic(args)
    else:
        load_tpch(args)


if __name__ == "__main__":
    main()
