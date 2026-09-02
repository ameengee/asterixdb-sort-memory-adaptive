#!/usr/bin/env python3
"""Cached vs I/O-bound: why 'more memory helps' depends entirely on the regime.

Page-cache-resident, more memory buys almost nothing -- there is no I/O to save, and larger runs
only mean more comparisons. Once reads reach the device, a larger budget means fewer runs, fewer
merge passes, and less disk traffic, and our curve turns sharply downward.

This also shows stock's U-curve FLATTENING under I/O pressure: at small budgets stock is already
I/O-bound, which masks the CPU penalty that produces the curve in the cached case. Worth being
explicit about in the paper -- the U-curve is a CPU-bound phenomenon, not a universal one.

usage: make_io_figure.py <cached:sixarm times> <cold:iotest times> <out.png>
"""
import sys, statistics as st
from collections import defaultdict
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

cached_src, cold_src, dst = sys.argv[1], sys.argv[2], sys.argv[3]

def load(fn, tag, armcol=2, memcol=3, valcol=4):
    d = defaultdict(list)
    for line in open(fn):
        f = line.split()
        if len(f) == 5 and f[0] == tag:
            d[(f[armcol], f[memcol])].append(float(f[valcol]))
    return d

cached = load(cached_src, "SA")
cold = load(cold_src, "IO")

def mb(m): return int(m.replace("MB", ""))

# Only budgets measured in BOTH regimes can be compared.
mems = sorted({m for _, m in cached} & {m for _, m in cold}, key=mb)
if not mems:
    sys.exit("no overlapping memory levels between the two runs")

SERIES = [
    ("stock-clone",  cached, "stock, cached",      "#9ca3af", "o", "--"),
    ("bucket-best",  cached, "ours, cached",       "#93c5fd", "D", "--"),
    ("stock",        cold,   "stock, I/O-bound",   "#374151", "o", "-"),
    ("ours",         cold,   "ours, I/O-bound",    "#1d4ed8", "D", "-"),
]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12.5, 5.2))
print(f"{'series':<20}" + "".join(f"{m:>10}" for m in mems) + f"{'slope':>9}")
for arm, src, label, color, marker, ls in SERIES:
    med = [st.median(src[(arm, m)]) for m in mems if src.get((arm, m))]
    if len(med) != len(mems):
        print(f"{label:<20} (missing cells)"); continue
    xs = [mb(m) for m in mems]
    print(f"{label:<20}" + "".join(f"{v:>10.2f}" for v in med) + f"{(med[-1]/med[0]-1)*100:>8.1f}%")
    kw = dict(color=color, marker=marker, ls=ls, lw=2, ms=6)
    ax1.plot(xs, med, label=label, **kw)
    ax2.plot(xs, [v / med[0] for v in med], label=label, **kw)

for ax, ylab, title in ((ax1, "Sort time (s), median", "Absolute cost"),
                        (ax2, "Relative to that series at the smallest budget",
                         "What another GB of memory buys")):
    ax.set_xscale("log", base=2)
    ax.set_xticks([mb(m) for m in mems])
    ax.set_xticklabels([m.replace("MB", "") for m in mems])
    ax.set_xlabel("Sort memory budget (MB)")
    ax.set_ylabel(ylab); ax.set_title(title); ax.grid(alpha=0.3, ls=":")
ax2.axhline(1.0, color="black", lw=0.8, alpha=0.5)
ax1.legend(frameon=False, fontsize=9)
fig.suptitle("More memory only pays when the sort actually touches disk", fontsize=12, y=0.99)
fig.tight_layout()
fig.savefig(dst, dpi=160)
print("wrote", dst)
