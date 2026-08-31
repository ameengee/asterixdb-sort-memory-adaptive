#!/usr/bin/env python3
"""
Memory-adaptive sort -- E1 ("no harm") analysis.

Compares the stock and adaptive arms on query latency.

Resource-shape questions are NOT answered here: process-level CPU sampling cannot separate loading
from sorting (both are ~100% busy on the operator thread). Use the sorter's own phase counters
(-Dhyracks.sort.phaseLog=true) plus make_figures.py for that.

Because blocks alternate (stock, adaptive, stock, adaptive, ...), this also reports each arm
per round, so machine drift is visible instead of being silently folded into the comparison.

Usage:  ./analyze_e1.py --outdir results
"""

import argparse
import csv
import glob
import os
import statistics as st


def pct(xs, p):
    if not xs:
        return float("nan")
    xs = sorted(xs)
    k = min(len(xs) - 1, max(0, int(round((len(xs) - 1) * p))))
    return xs[k]


def load_latency(path):
    with open(path) as fh:
        rows = [r for r in csv.DictReader(fh) if r.get("ok") == "True"]
    return [float(r["clientElapsedMs"]) for r in rows]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default="results")
    args = ap.parse_args()

    arms = {}
    for path in sorted(glob.glob(os.path.join(args.outdir, "e1-*-r*.csv"))):
        base = os.path.basename(path)
        if ".pressure." in base or ".decisions." in base or ".runs." in base or ".broker." in base:
            continue
        label = base[:-4]
        parts = label.split("-")
        arm, rnd = parts[1], parts[-1]
        arms.setdefault(arm, {})[rnd] = path

    if not arms:
        print("no E1 result files found in", args.outdir)
        return

    print("=" * 78)
    print("E1 -- NO HARM:  stock (master) vs adaptive (inert broker)")
    print("=" * 78)

    print(f"\n{'arm':10} {'round':6} {'n':>6} {'p50':>9} {'p95':>9} {'p99':>9} {'mean':>9}")
    print("-" * 78)
    pooled = {}
    for arm in sorted(arms):
        for rnd in sorted(arms[arm]):
            xs = load_latency(arms[arm][rnd])
            pooled.setdefault(arm, []).extend(xs)
            print(f"{arm:10} {rnd:6} {len(xs):>6} {pct(xs,0.50):>9.1f} {pct(xs,0.95):>9.1f} "
                  f"{pct(xs,0.99):>9.1f} {st.mean(xs) if xs else float('nan'):>9.1f}")

    print("\n" + "-" * 78)
    print("POOLED (all rounds)   latency in ms")
    print("-" * 78)
    print(f"{'arm':10} {'n':>6} {'p50':>9} {'p95':>9} {'p99':>9} {'mean':>9} {'stdev':>9}")
    for arm in sorted(pooled):
        xs = pooled[arm]
        print(f"{arm:10} {len(xs):>6} {pct(xs,0.50):>9.1f} {pct(xs,0.95):>9.1f} "
              f"{pct(xs,0.99):>9.1f} {st.mean(xs):>9.1f} {st.pstdev(xs):>9.1f}")

    if "stock" in pooled and "adaptive" in pooled:
        s, a = pooled["stock"], pooled["adaptive"]
        print("\n" + "-" * 78)
        print("DELTA (adaptive vs stock; negative = adaptive faster)")
        print("-" * 78)
        for name, f in (("p50", lambda x: pct(x, 0.50)), ("p95", lambda x: pct(x, 0.95)),
                        ("p99", lambda x: pct(x, 0.99)), ("mean", st.mean)):
            sv, av = f(s), f(a)
            print(f"  {name:5} stock={sv:8.1f}ms  adaptive={av:8.1f}ms  "
                  f"delta={av - sv:+8.1f}ms ({(av - sv) / sv * 100:+.2f}%)")


if __name__ == "__main__":
    main()
