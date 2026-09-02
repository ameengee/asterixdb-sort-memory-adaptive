#!/usr/bin/env python3
"""The paper's headline figure: six arms across sort memory.

Left  = absolute time, so the reader sees stock's U-curve and how far below it we sit.
Right = each arm normalized to its own best, which is the only way to read SLOPE -- the
        question "does more memory help this configuration?" is about curve shape, not height.

Reads:  SA <round> <arm> <mem> <seconds>
"""
import sys, statistics as st
from collections import defaultdict
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

SRC = sys.argv[1] if len(sys.argv) > 1 else "results/sixarm/times.txt"
DST = sys.argv[2] if len(sys.argv) > 2 else "figures/sixarm.png"

# Order matters: this is the order the paper walks through them.
ARMS = [
    ("stock-clone",    "1. Stock AsterixDB (as cloned)",        "#6b7280", "o", "-"),
    ("stock-mixed",    "2. Stock, multi-type column",           "#991b1b", "v", "-"),
    ("bucket-plain",   "3. Bucketing, no type fix",             "#c2410c", "^", "--"),
    ("stock-typefix",  "4. Stock + our type detection",         "#0f766e", "s", "--"),
    ("bucket-typefix", "5. Bucketing + type detection",         "#1d4ed8", "D", "-"),
    ("bucket-best",    "6. Everything on (best)",               "#4d7c0f", "*", "-"),
]

def mb(m): return int(m.replace("MB", ""))

data = defaultdict(list)
for line in open(SRC):
    f = line.split()
    if len(f) == 5 and f[0] == "SA":
        data[(f[2], f[3])].append(float(f[4]))

mems = sorted({m for _, m in data}, key=mb)
if not mems:
    sys.exit(f"no samples parsed from {SRC}")

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5.6))
series = []
print(f"{'arm':<16}" + "".join(f"{m:>9}" for m in mems) + f"{'slope':>9}{'n':>5}")
for arm, label, color, marker, ls in ARMS:
    xs, med = [], []
    for m in mems:
        v = data.get((arm, m), [])
        if v:
            xs.append(mb(m)); med.append(st.median(v))
    if not xs:
        print(f"{arm:<16} (no data)")
        continue
    n = min(len(data[(arm, m)]) for m in mems if data.get((arm, m)))
    print(f"{arm:<16}" + "".join(f"{v:>9.2f}" for v in med)
          + f"{(med[-1]/med[0]-1)*100:>8.1f}%{n:>5}")
    kw = dict(color=color, marker=marker, ls=ls, lw=2, ms=7 if marker != "*" else 11)
    ax1.plot(xs, med, label=label, **kw)
    ax2.plot(xs, [t / min(med) for t in med], label=label, **kw)
    series.append((arm, label, color, marker, ls, xs, med))

for ax, ylab, title in (
        (ax1, "Sort time (s), median", "What it costs"),
        (ax2, "Relative to that arm's own best", "Does more memory help?")):
    ax.set_xscale("log", base=2)
    ax.set_xticks([mb(m) for m in mems])
    ax.set_xticklabels([m.replace("MB", "") for m in mems])
    ax.set_xlabel("Sort memory budget (MB)")
    ax.set_ylabel(ylab); ax.set_title(title)
    ax.grid(alpha=0.3, ls=":")
ax2.axhline(1.0, color="black", lw=0.8, alpha=0.5)

# The three fixed arms sit within ~0.6s of each other; on an axis that must also show stock at 9s
# they collapse into one line. Inset them at their own scale so the comparison is actually visible.
FAST = {"stock-typefix", "bucket-typefix", "bucket-best"}
fast = [t for t in series if t[0] in FAST]
if fast:
    axin = ax1.inset_axes([0.46, 0.26, 0.51, 0.36])
    for _, label, color, marker, ls, xs, med in fast:
        axin.plot(xs, med, color=color, marker=marker, ls=ls, lw=1.6,
                  ms=5 if marker != "*" else 8)
    axin.set_xscale("log", base=2)
    axin.set_xticks([mb(m) for m in mems])
    axin.set_xticklabels([m.replace("MB", "") for m in mems], fontsize=7)
    axin.tick_params(labelsize=7)
    axin.grid(alpha=0.3, ls=":")
    axin.set_title("arms 4-6, zoomed", fontsize=8, pad=3)
    axin.patch.set_alpha(0.95)
    for sp in axin.spines.values():
        sp.set_alpha(0.4)
ax1.legend(frameon=False, fontsize=8.5, loc="upper left")
fig.suptitle("10M-row external sort: stock's U-curve, and what removes it", fontsize=12, y=0.99)
fig.tight_layout()
fig.savefig(DST, dpi=160)
print("wrote", DST)
