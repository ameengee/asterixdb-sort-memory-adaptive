#!/usr/bin/env python3
"""290M rows (7.3GB) with the page cache squeezed to ~1-4GB: the I/O-bound regime.

Left  : absolute time. Stock's U-curve is LARGER here (+54%) than in the cached case (+39%),
        so the curve is not an artifact of everything fitting in memory.
Right : each arm against its own 32MB time. Ours rises only +11.6% versus stock's +54.3% --
        we degrade gracefully rather than improve. At 1.1GB, where a 2GB budget exceeded the
        whole dataset, ours instead FELL 23.8%; the governing variable is the memory-to-data
        ratio, not the presence of I/O.
"""
import sys, statistics as st
from collections import defaultdict
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

SRC = sys.argv[1] if len(sys.argv) > 1 else "results/iotest-big/times.txt"
DST = sys.argv[2] if len(sys.argv) > 2 else "figures/bigio.png"
ROUND = sys.argv[3] if len(sys.argv) > 3 else "1"   # round 2 ran under degraded pressure

d = defaultdict(list)
for line in open(SRC):
    f = line.split()
    if len(f) == 5 and f[0] == "IO" and f[1] == ROUND:
        d[(f[2], f[3])].append(float(f[4]))

mems = ["32MB", "512MB", "2048MB"]
def mb(m): return int(m.replace("MB", ""))
xs = [mb(m) for m in mems]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12.5, 5.2))
for arm, label, color, marker in (("stock", "Stock AsterixDB", "#374151", "o"),
                                  ("ours", "Ours (type detection + bucketing)", "#1d4ed8", "D")):
    med = [st.median(d[(arm, m)]) for m in mems]
    lo = [min(d[(arm, m)]) for m in mems]
    hi = [max(d[(arm, m)]) for m in mems]
    print(f"{label:<36} " + "  ".join(f"{m}={v:.0f}s" for m, v in zip(mems, med))
          + f"   slope={(med[-1]/med[0]-1)*100:+.1f}%")
    ax1.plot(xs, med, color=color, marker=marker, lw=2, ms=7, label=label)
    ax1.fill_between(xs, lo, hi, color=color, alpha=0.15, lw=0)
    ax2.plot(xs, [v / med[0] for v in med], color=color, marker=marker, lw=2, ms=7, label=label)

for ax, ylab, title in ((ax1, "Sort time (s), median of 2", "290M rows, 7.3 GB, cache squeezed to ~1-4 GB"),
                        (ax2, "Relative to that arm at 32MB", "How gracefully each degrades")):
    ax.set_xscale("log", base=2)
    ax.set_xticks(xs); ax.set_xticklabels([m.replace("MB", "") for m in mems])
    ax.set_xlabel("Sort memory budget (MB)"); ax.set_ylabel(ylab)
    ax.set_title(title, fontsize=11); ax.grid(alpha=0.3, ls=":")
ax2.axhline(1.0, color="black", lw=0.8, alpha=0.5)
ax1.legend(frameon=False, fontsize=9)
for i, m in enumerate(mems):
    sp = st.median(d[("stock", m)]) / st.median(d[("ours", m)])
    ax1.annotate(f"{sp:.2f}x", (xs[i], st.median(d[("ours", m)])), textcoords="offset points",
                 xytext=(0, -18), ha="center", fontsize=9, color="#1d4ed8")
fig.suptitle("I/O-bound sort at 7.3 GB: stock's curve steepens, ours stays nearly flat", fontsize=12, y=0.99)
fig.tight_layout(); fig.savefig(DST, dpi=160)
print("wrote", DST)
