#!/usr/bin/env python3
"""
Memory-adaptive sort -- E1 ("no harm") analysis.

Compares the stock and adaptive arms on latency, and summarizes the CPU/disk pressure time
series that shows *how* the work is distributed (the sawtooth question).

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


def load_pressure(path):
    cpu, wr, rd = [], [], []
    if not os.path.exists(path):
        return cpu, wr, rd
    with open(path) as fh:
        for r in csv.DictReader(fh):
            try:
                cpu.append(float(r["cpuPctTotal"]))
                wr.append(float(r["writeMBps"] or 0))
                rd.append(float(r["readMBps"] or 0))
            except (ValueError, KeyError):
                continue
    return cpu, wr, rd


def burstiness(xs):
    """Coefficient of variation. Higher = spikier (the sawtooth); lower = smoother."""
    xs = [x for x in xs if x is not None]
    if len(xs) < 2:
        return float("nan")
    m = st.mean(xs)
    if m == 0:
        return float("nan")
    return st.pstdev(xs) / m


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

    print("\n" + "=" * 78)
    print("PRESSURE  (cv = coefficient of variation; HIGHER = burstier/sawtooth)")
    print("=" * 78)
    print(f"{'arm':10} {'samples':>8} {'cpu_mean':>9} {'cpu_max':>9} {'cpu_cv':>8} "
          f"{'wr_mean':>9} {'wr_cv':>8}")
    print("-" * 78)
    for arm in sorted(arms):
        allcpu, allwr = [], []
        for rnd in sorted(arms[arm]):
            p = arms[arm][rnd].replace(".csv", ".pressure.csv")
            cpu, wr, _ = load_pressure(p)
            allcpu += cpu
            allwr += wr
        if not allcpu:
            print(f"{arm:10} {'(no pressure samples)':>30}")
            continue
        print(f"{arm:10} {len(allcpu):>8} {st.mean(allcpu):>9.1f} {max(allcpu):>9.1f} "
              f"{burstiness(allcpu):>8.3f} {st.mean(allwr):>9.3f} {burstiness(allwr):>8.3f}")

    print("\nNote: cv compares the SHAPE of resource use. The claim under test is that the")
    print("incremental bucket sort spreads CPU across data arrival, lowering cpu_cv relative")
    print("to stock's load-then-sort sawtooth.")


if __name__ == "__main__":
    main()
