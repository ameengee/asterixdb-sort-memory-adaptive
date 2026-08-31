#!/usr/bin/env python3
"""
Figures for the sort-memory sweep (E1 "no harm" + U-curve).

Input: results/noharm/timings.txt, lines of the form
    TIME <arm> <level> r<round>_<rep> <seconds>

Produces:
  fig5_memory_sweep.{pdf,png}  absolute query time vs sort memory, one line per arm, 95% CI
  fig6_vs_stock.{pdf,png}      each adaptive arm as % difference from stock, with a zero line

Why a log x-axis: the levels are 0.1x / 1x / 10x / ~60x of AsterixDB's 32MB default, so they are
multiplicative. A linear axis would bunch the three small levels together and hide the shape.
"""

import argparse
import collections
import math
import re
import statistics as st
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

STYLE = {
    "stock":       ("#3B6EA5", "o", "stock AsterixDB"),
    "adapt-eager": ("#7A9E3F", "s", "adaptive, eager cascade (fan-in 2)"),
    "adapt-kway":  ("#C2571A", "^", "adaptive, k-way merge (large fan-in)"),
}
# label -> megabytes, for the x axis
LEVEL_MB = {"512KB": 0.5, "1MB": 1.0, "3200KB": 3.2, "32MB": 32.0, "320MB": 320.0,
            "2048MB": 2048.0}
DEFAULT_MB = 32.0  # AsterixDB's compiler.sortmemory default


def load(path, discard=0):
    """Group trials by (arm, level), optionally dropping the first `discard` trials OF EACH ROUND.

    Why a discard rule is needed: the adaptive jar carries more code to JIT than stock, and the
    single warmup query each cell already performs is not always enough at high sort memory. The
    per-trial series shows it plainly -- e.g. adapt-kway at 320MB reads
    19.4, 17.0, 15.7, 15.5, 15.6, 15.6, ... : two trials still warming, then flat.
    Stock is flat from trial 1, so leaving the tail in UNDERSTATES the adaptive arms.

    The rule is applied UNIFORMLY to every arm and level -- never per-cell -- so it cannot be
    tuned to favour an arm. Report the discard value alongside the numbers, and check the
    sensitivity table before choosing one.
    """
    d = collections.defaultdict(lambda: collections.defaultdict(lambda: collections.defaultdict(list)))
    for line in open(path):
        if not line.startswith("TIME"):
            continue
        parts = line.split()
        if len(parts) < 5:
            continue
        _, arm, level, tag, t = parts[:5]
        rnd = tag.split("_")[0]
        d[arm][level][rnd].append(float(t))
    out = collections.defaultdict(lambda: collections.defaultdict(list))
    for arm in d:
        for level in d[arm]:
            for rnd, vals in d[arm][level].items():
                out[arm][level].extend(vals[discard:])
    return out


def sensitivity(path, arms, levels):
    """Show how each arm's delta vs stock moves as the discard count changes."""
    print("\nDISCARD SENSITIVITY (delta vs stock, %)")
    hdr = "  discard" + "".join(f"{l:>12}" for l in levels)
    for arm in arms:
        if arm == "stock":
            continue
        print(f"\n  {arm}")
        print(hdr)
        for k in (0, 1, 2, 3):
            dk = load(path, discard=k)
            row = f"{k:>9}"
            for l in levels:
                s_ = st.median(dk["stock"][l]) if dk["stock"][l] else float("nan")
                a_ = st.median(dk[arm][l]) if dk[arm][l] else float("nan")
                row += f"{100*(a_-s_)/s_:>11.1f}%" if s_ == s_ and a_ == a_ else f"{'--':>12}"
            print(row)


def ci95(xs):
    """Half-width of the 95% CI of the mean. n=40 makes this tight enough to see ~0.5%."""
    if len(xs) < 2:
        return 0.0
    return 1.96 * st.pstdev(xs) / math.sqrt(len(xs))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--timings", default="results/noharm/timings.txt")
    ap.add_argument("--outdir", default="figures")
    ap.add_argument("--discard", type=int, default=2,
                    help="drop the first N trials of EACH round (uniform across all arms)")
    args = ap.parse_args()

    data = load(args.timings, discard=args.discard)
    arms = [a for a in ("stock", "adapt-eager", "adapt-kway") if a in data]
    levels = sorted({l for a in data for l in data[a]}, key=lambda l: LEVEL_MB.get(l, 1e9))
    xs = [LEVEL_MB[l] for l in levels]

    # ---------- fig 5: absolute ----------
    fig, ax = plt.subplots(figsize=(7.0, 4.3))
    for arm in arms:
        colour, marker, label = STYLE[arm]
        ys = [st.median(data[arm][l]) for l in levels]
        es = [ci95(data[arm][l]) for l in levels]
        ax.errorbar(xs, ys, yerr=es, marker=marker, ms=6, lw=1.9, capsize=4,
                    color=colour, label=label)
    ax.axvline(DEFAULT_MB, color="#888888", lw=1, ls=":")
    # annotate low, near the axis, so it does not read as a second title
    lo, hi = ax.get_ylim()
    ax.annotate("AsterixDB\ndefault", xy=(DEFAULT_MB * 1.12, lo + (hi - lo) * 0.04),
                fontsize=8, color="#666666", va="bottom")
    ax.set_xscale("log")
    ax.set_xticks(xs, [l.replace("3200KB", "3.2MB").replace("2048MB", "2GB") for l in levels])
    ax.get_xaxis().set_minor_formatter(plt.NullFormatter())
    ax.set_xlabel("sort memory per operator per partition (log scale)")
    ax.set_ylabel("query time (seconds)")
    ax.set_title("Query time vs sort memory", fontsize=12, loc="left")
    ax.grid(True, color="#DDDDDD", lw=0.7)
    ax.set_axisbelow(True)
    ax.legend(fontsize=8.5, framealpha=0.95, loc="upper center",
              bbox_to_anchor=(0.5, -0.16), ncol=3, frameon=False)
    n = min(len(data[a][l]) for a in arms for l in levels)
    fig.text(0.5, 0.015, f"Median of {n} trials per point; bars are 95% CI of the mean. "
             "10M rows, 2 partitions.", ha="center", fontsize=7.5, color="#555555")
    fig.tight_layout(rect=[0, 0.13, 1, 1])
    for ext in ("pdf", "png"):
        fig.savefig(f"{args.outdir}/fig5_memory_sweep.{ext}", dpi=200)
    plt.close(fig)

    # ---------- fig 6: relative to stock ----------
    fig, ax = plt.subplots(figsize=(7.0, 4.0))
    base = [st.median(data["stock"][l]) for l in levels]
    for arm in arms:
        if arm == "stock":
            continue
        colour, marker, label = STYLE[arm]
        ys = [100.0 * (st.median(data[arm][l]) - b) / b for l, b in zip(levels, base)]
        ax.plot(xs, ys, marker=marker, ms=6, lw=1.9, color=colour, label=label)
    ax.axhline(0, color="#3B6EA5", lw=1.4, ls="--", label="stock (baseline)")
    ax.axvline(DEFAULT_MB, color="#888888", lw=1, ls=":")
    ax.set_xscale("log")
    ax.set_xticks(xs, [l.replace("3200KB", "3.2MB").replace("2048MB", "2GB") for l in levels])
    ax.get_xaxis().set_minor_formatter(plt.NullFormatter())
    ax.set_xlabel("sort memory per operator per partition (log scale)")
    ax.set_ylabel("query time vs stock (%)")
    ax.set_title("Speedup over stock, by sort memory", fontsize=12, loc="left")
    ax.grid(True, color="#DDDDDD", lw=0.7)
    ax.set_axisbelow(True)
    ax.legend(fontsize=8.5, framealpha=0.95)
    fig.text(0.5, 0.015, "Below the dashed line is faster than stock.",
             ha="center", fontsize=7.5, color="#555555")
    fig.tight_layout(rect=[0, 0.06, 1, 1])
    for ext in ("pdf", "png"):
        fig.savefig(f"{args.outdir}/fig6_vs_stock.{ext}", dpi=200)
    plt.close(fig)

    # ---------- console summary ----------
    print(f"{'level':>9}" + "".join(f"{a:>16}" for a in arms) + f"{'best arm':>14}")
    for i, l in enumerate(levels):
        row = f"{l:>9}"
        for a in arms:
            v = data[a][l]
            row += f"{st.median(v):>10.2f}±{ci95(v):<5.2f}"
        best = min(arms, key=lambda a: st.median(data[a][l]))
        row += f"{best:>14}"
        print(row)
    print(f"\nstock relative to its own best point (U-curve check):")
    smin = min(st.median(data['stock'][l]) for l in levels)
    for l in levels:
        m = st.median(data['stock'][l])
        print(f"  {l:>9}: {m:6.2f}s  {100*(m-smin)/smin:+6.1f}%")
    sensitivity(args.timings, arms, levels)
    print(f"\n(discard={args.discard} trials per round, applied uniformly to every arm)")
    print(f"wrote {args.outdir}/fig5_memory_sweep.*, {args.outdir}/fig6_vs_stock.*")


if __name__ == "__main__":
    main()
