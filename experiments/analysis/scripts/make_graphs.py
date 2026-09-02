#!/usr/bin/env python3
"""Render the 12 analysis graphs from a matrix run.

Input lines:  MX <round> <config> <dataset> <mem> <seconds>
Configs: A stock | B auto-key no bucketing | C bucketing no type fix
         D bucketing + auto-key | E bucketing + auto-key + k-way (ideal)

usage: make_graphs.py <times.txt> <outdir> <small|large>
"""
import sys, os, statistics as st
from collections import defaultdict
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

SRC, OUTDIR, SCALE = sys.argv[1], sys.argv[2], sys.argv[3]
os.makedirs(OUTDIR, exist_ok=True)
SINGLE, TYPED, MULTI = ("test", "typed", "mixbig") if SCALE == "small" else ("big", "bigtyped", "bigmixed")
N0 = 1 if SCALE == "small" else 7          # graph numbering offset
SIZE = "~600 MB (10M rows)" if SCALE == "small" else "~7 GB (290M rows)"

data = defaultdict(list)
for line in open(SRC):
    f = line.split()
    if len(f) == 6 and f[0] == "MX":
        data[(f[2], f[3], f[4])].append(float(f[5]))

mems = sorted({k[2] for k in data}, key=lambda m: int(m.replace("MB", "")))
def mb(m): return int(m.replace("MB", ""))
def med(cfg, dv):
    out = []
    for m in mems:
        v = data.get((cfg, dv, m))
        if not v: return None
        out.append(st.median(v))
    return out

NOTYPE = ("No type declared",   "#b91c1c", "o", "-")
USER   = ("User declares type", "#0f766e", "s", "--")
AUTO   = ("Auto-detected type", "#1d4ed8", "D", "-")

def lineplot(fname, title, subtitle, series, impossible=None, ratio_to=None):
    """series: list of (label, color, marker, ls, medians)."""
    fig, ax = plt.subplots(figsize=(8.6, 5.4))
    plotted = []
    for label, color, marker, ls, ys in series:
        if ys is None: continue
        y = [a / b for a, b in zip(ys, ratio_to)] if ratio_to else ys
        ax.plot([mb(m) for m in mems], y, label=label, color=color, marker=marker,
                ls=ls, lw=2, ms=7)
        plotted.append((color, y))
        pct = (y[-1] / y[0] - 1) * 100
        print(f"    {label:<22} " + "  ".join(f"{m}={v:.2f}" for m, v in zip(mems, y))
              + f"   slope={pct:+.1f}%")
    fmt = (lambda v: f"{v:.3f}") if ratio_to else (lambda v: f"{v:.2f}")
    for xi, m in enumerate(mems):
        col = sorted(((yv[xi], c) for c, yv in plotted), key=lambda t: t[0])
        for rank, (yval, color) in enumerate(col):
            # lowest label below the point, the rest stacked upward at widening offsets
            dy = -15 if rank == 0 else 9 + 13 * (rank - 1)
            ax.annotate(fmt(yval), (mb(m), yval), textcoords="offset points",
                        xytext=(0, dy), ha="center", fontsize=7.5, color=color,
                        bbox=dict(boxstyle="round,pad=0.18", fc="white", ec="none", alpha=0.75))
    ax.margins(y=0.16)
    if ratio_to:
        ax.axhline(1.0, color="black", lw=1.0, alpha=0.6)
        ax.set_ylabel("Time relative to stock (1.0 = parity)")
    else:
        ax.set_ylabel("Sort time (s), median")
    if impossible:
        # The absence is the argument: say so on the figure instead of quietly dropping a line.
        ax.plot([], [], color=USER[1], ls="--", marker="s", label=impossible)
        ax.text(0.5, 0.03, impossible, transform=ax.transAxes, ha="center", fontsize=9,
                color=USER[1], style="italic",
                bbox=dict(boxstyle="round,pad=0.4", fc="#ecfdf5", ec=USER[1], alpha=0.9))
    ax.set_xscale("log", base=2)
    ax.set_xticks([mb(m) for m in mems]); ax.set_xticklabels([m.replace("MB", "") for m in mems])
    ax.set_xlabel("Sort memory budget (MB)")
    ax.set_title(title, fontsize=12)
    ax.set_title(subtitle, fontsize=9, loc="right", color="#6b7280")
    ax.grid(alpha=0.3, ls=":"); ax.legend(frameon=False, fontsize=9)
    fig.tight_layout(); fig.savefig(os.path.join(OUTDIR, fname), dpi=160); plt.close(fig)
    print(f"  wrote {fname}")

IMP = "Not expressible in AsterixDB:\na column cannot be declared with several types"

specs = [
    (N0+0, "g%d_stock_single.png",  "Stock AsterixDB, single-type column",
     [(*NOTYPE, med("A", SINGLE)), (*USER, med("A", TYPED)), (*AUTO, med("B", SINGLE))], None, None),
    (N0+1, "g%d_stock_multi.png",   "Stock AsterixDB, multi-type column",
     [(*NOTYPE, med("A", MULTI)), (*AUTO, med("B", MULTI))], IMP, None),
    (N0+2, "g%d_bucket_single.png", "Bucketed sort, single-type column",
     [(*NOTYPE, med("C", SINGLE)), (*USER, med("C", TYPED)), (*AUTO, med("D", SINGLE))], None, None),
    (N0+3, "g%d_bucket_multi.png",  "Bucketed sort, multi-type column",
     [(*NOTYPE, med("C", MULTI)), (*AUTO, med("D", MULTI))], IMP, None),
    (N0+4, "g%d_default_vs_best.png", "As-cloned AsterixDB vs our best, multi-type column",
     [("Stock AsterixDB (as cloned)", "#b91c1c", "o", "-", med("A", MULTI)),
      ("Ours, ideal settings", "#1d4ed8", "D", "-", med("E", MULTI))], None, None),
]
print(f"\n=== {SIZE} ===")
for n, fname, title, series, imp, _ in specs:
    print(f"\nGraph {n}: {title}")
    lineplot(fname % n, title, SIZE, series, impossible=imp)

# Graph 6/12: where stock is STRONGEST (perfectly declared types). Plotted as a ratio, because two
# near-identical curves hide the only thing worth seeing -- how bounded the harm is.
base = med("A", TYPED)
print(f"\nGraph {N0+5}: bounded harm where stock is strongest (typed column), ratio to stock")
if base:
    lineplot(f"g{N0+5}_no_harm_ratio.png",
             "Where AsterixDB is strongest: how much do we cost?",
             SIZE,
             [("Stock + user-declared type", "#0f766e", "s", "--", base),
              ("Ours, ideal settings", "#1d4ed8", "D", "-", med("E", TYPED))],
             ratio_to=base)
else:
    print("  (no typed-dataset data yet)")
