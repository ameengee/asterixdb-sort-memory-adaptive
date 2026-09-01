#!/usr/bin/env python3
"""Three-line figure: stock / stock+hand-written schema / ours, across sort memory.

Reads times.txt lines:  TL <round> <arm> <mem> <seconds>
Left panel  = absolute time.  Right panel = normalized to each arm's own best,
which is what actually answers "does more memory help THIS sorter?".
"""
import sys, statistics as st
from collections import defaultdict
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

SRC = sys.argv[1] if len(sys.argv) > 1 else "results/threeline/times.txt"
DST = sys.argv[2] if len(sys.argv) > 2 else "figures/threeline.png"

ARMS = [("stock",       "Stock AsterixDB (untyped column)",        "#6b7280", "o", "-"),
        ("stock-typed", "Stock + hand-written schema",             "#b45309", "s", "--"),
        ("ours",        "Ours: auto-detected key + bucketing",     "#1d4ed8", "D", "-")]

def mb(m): return int(m.replace("MB", ""))

data = defaultdict(list)
for line in open(SRC):
    f = line.split()
    if len(f) == 5 and f[0] == "TL":
        data[(f[2], f[3])].append(float(f[4]))

mems = sorted({m for _, m in data}, key=mb)
if not mems:
    sys.exit(f"no samples parsed from {SRC}")

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5.2))

print(f"{'arm':<12} {'mem':>8} {'n':>3} {'median':>8} {'p25':>7} {'p75':>7}")
for arm, label, color, marker, ls in ARMS:
    xs, med, lo, hi = [], [], [], []
    for m in mems:
        v = sorted(data.get((arm, m), []))
        if not v:
            continue
        q = st.quantiles(v, n=4) if len(v) >= 4 else [v[0], st.median(v), v[-1]]
        xs.append(mb(m)); med.append(st.median(v)); lo.append(q[0]); hi.append(q[2])
        print(f"{arm:<12} {m:>8} {len(v):>3} {st.median(v):>8.2f} {q[0]:>7.2f} {q[2]:>7.2f}")
    if not xs:
        continue
    ax1.plot(xs, med, marker=marker, ls=ls, color=color, label=label, lw=2, ms=6)
    ax1.fill_between(xs, lo, hi, color=color, alpha=0.15, lw=0)
    base = min(med)
    ax2.plot(xs, [t / base for t in med], marker=marker, ls=ls, color=color, label=label, lw=2, ms=6)

for ax, ylab, title in ((ax1, "Sort time (s), median of all trials", "Absolute cost"),
                        (ax2, "Slowdown vs. that arm's own best", "Does more memory help?")):
    ax.set_xscale("log", base=2)
    ax.set_xticks([mb(m) for m in mems])
    ax.set_xticklabels([m.replace("MB", "") for m in mems])
    ax.set_xlabel("Sort memory budget (MB)")
    ax.set_ylabel(ylab)
    ax.set_title(title)
    ax.grid(alpha=0.3, ls=":")
ax2.axhline(1.0, color="black", lw=0.8, alpha=0.5)
ax1.legend(frameon=False, fontsize=9)
fig.suptitle("10M-row external sort: what the user gets, and what more memory buys",
             fontsize=12, y=0.99)
fig.tight_layout()
fig.savefig(DST, dpi=160)
print("wrote", DST)
