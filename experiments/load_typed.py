#!/usr/bin/env python3
"""
Load datasets that differ ONLY in whether the sort key's type is declared.

This is the T1/T2 discriminator.

  test.ds      open type, `k` UNDECLARED  -> static type of r.k is ANY
  typed.ds     CLOSED type, every column declared

Why it matters: JobGenHelper.variablesToAscNormalizedKeyComputerFactory asks the type environment
for the sort variable's type and hands it to NormalizedKeyComputerFactoryProvider, whose switch
ends in `default: return null`. An undeclared field of an `open` type has static type ANY, which
hits that default -- so the sorter would get NO normalized key, and every compare() would
dereference tuple bytes via bufferManager.getFrame() and run the full binary comparator.

The closed dataset declares several key types so we can also test Ameen's string theory:
  k_big  BIGINT  -> 2-int normalized key, DECISIVE (comparator never runs)
  k_int  INTEGER -> 1-int, DECISIVE
  k_dbl  DOUBLE  -> 2-int, DECISIVE
  k_str  STRING  -> 1-int prefix, INDECISIVE (always falls through to the comparator)

Both datasets carry identical values, so any timing difference is attributable to key handling
alone rather than to the data.

Usage:  ./load_typed.py --rows 10000000
"""
import argparse, json, sys, time, urllib.parse, urllib.request

def q(ep, stmt, timeout=7200):
    data = urllib.parse.urlencode({"statement": stmt}).encode()
    with urllib.request.urlopen(ep, data=data, timeout=timeout) as r:
        p = json.load(r)
    if p.get("status") != "success":
        raise RuntimeError(json.dumps(p.get("errors", p))[:600])
    return p

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", default="http://127.0.0.1:19002/query/service")
    ap.add_argument("--rows", type=int, default=10_000_000)
    ap.add_argument("--batch", type=int, default=1_000_000)
    ap.add_argument("--pad-bytes", type=int, default=100)
    args = ap.parse_args()
    ep = args.endpoint
    pad = "x" * args.pad_bytes

    print("[load] creating dataverse `typed` with a CLOSED type")
    q(ep, """
        DROP DATAVERSE typed IF EXISTS;
        CREATE DATAVERSE typed;
        USE typed;
        CREATE TYPE t AS closed {
          id: int64, k: int64, k_int: int32, k_dbl: double, k_str: string, payload: string
        };
        CREATE DATASET ds(t) PRIMARY KEY id;
    """)
    started = time.time(); done = 0
    while done < args.rows:
        lo, hi = done + 1, min(done + args.batch, args.rows)
        # identical value expressions to load_data.py so the two datasets hold the same data
        q(ep, f"""
            USE typed;
            INSERT INTO ds (
              SELECT VALUE {{
                "id": x,
                "k": (x * 48271) % 2000000,
                "k_int": int32((x * 48271) % 2000000),
                "k_dbl": ((x * 48271) % 2000000) / 7.0,
                "k_str": string((x * 48271) % 2000000) || "-key",
                "payload": "{pad}"
              }} FROM range({lo}, {hi}) AS x
            );
        """)
        done = hi
        print(f"[load] {done:,}/{args.rows:,}  ({time.time()-started:.0f}s)", flush=True)
    n = q(ep, "SELECT VALUE count(*) FROM typed.ds;")["results"][0]
    print(f"[load] typed.ds done: {n:,} rows in {time.time()-started:.0f}s")
    if n != args.rows:
        print(f"[load] WARNING expected {args.rows:,}", file=sys.stderr)

if __name__ == "__main__":
    main()
