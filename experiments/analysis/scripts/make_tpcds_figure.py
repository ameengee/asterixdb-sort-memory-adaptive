#!/usr/bin/env python3
"""The headline figure: a real TPC-DS fact table sorted under AsterixDB's own shipped schema.

Nothing here is synthetic. The data comes from the standard dsdgen, the schema is copied from
AsterixDB's test resources, and ss_sales_price is an ordinary measure column. It is declared
`double?` because TPC-DS measures genuinely can be null -- and a nullable type is a union, which the
normalized-key provider has no case for, so stock sorts it with no normalized key at all.
"""
import sys, statistics as st
from collections import defaultdict
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

SRC = sys.argv[1] if len(sys.argv) > 1 else "../data/tpcds_times.txt"
DST = sys.argv[2] if len(sys.argv) > 2 else "../figures/tpcds_headline.png"

d = defaultdict(list)
for line in open(SRC):
    f = line.split()
    if len(f) == 5 and f[0] == "TS":
        d[(f[2], f[3])].append(float(f[4]))

M = ["32MB", "128MB", "512MB", "2048MB"]
def mb(m): return int(m.replace("MB", ""))
xs = [mb(m) for m in M]

# The control matters: it shows the gap is caused by the DECLARATION, not by anything else about
# the data. Same 23 columns, same 28,800,991 rows, only `?` removed and nulls coalesced.
SERIES = [("stock-nullable", "Stock, official schema (double?)", "#b91c1c", "o", "-"),
          ("stock-nonnull",  "Stock, same data declared non-nullable", "#0f766e", "s", "--"),
          ("ours-nullable",  "Ours, official schema (auto-detected)", "#1d4ed8", "D", "-")]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13.5, 5.4))
meds = {}
for arm, label, color, marker, ls in SERIES:
    if not all(d.get((arm, m)) for m in M):
        print(f"  {label}: incomplete"); continue
    med = [st.median(d[(arm, m)]) for m in M]
    lo = [min(d[(arm, m)]) for m in M]; hi = [max(d[(arm, m)]) for m in M]
    meds[arm] = med
    print(f"  {label:<26} " + " ".join(f"{v:6.1f}s" for v in med)
          + f"   slope={(med[-1]/med[0]-1)*100:+.1f}%")
    ax1.plot(xs, med, color=color, marker=marker, ls=ls, lw=2.2, ms=8, label=label)
    ax1.fill_between(xs, lo, hi, color=color, alpha=0.15, lw=0)
    ax2.plot(xs, [v/med[0] for v in med], color=color, marker=marker, ls=ls, lw=2.2, ms=8, label=label)


# Value labels, stacked per budget in ascending order: the control and our line sit within ~0.5s of
# each other, so a fixed offset prints them on top of one another.
COLORS = {a: c for a, _, c, _, _ in SERIES}
for xi, m in enumerate(M):
    col = sorted(((meds[a][xi], COLORS[a]) for a in meds), key=lambda t: t[0])
    for rank, (val, color) in enumerate(col):
        dy = -16 if rank == 0 else 8 + 12 * (rank - 1)
        ax1.annotate(f"{val:.1f}s", (xs[xi], val), textcoords="offset points", xytext=(0, dy),
                     ha="center", fontsize=8, color=color,
                     bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.8))

# speedup annotations between the two curves
if "stock-nullable" in meds and "ours-nullable" in meds:
    for x, a, b in zip(xs, meds["stock-nullable"], meds["ours-nullable"]):
        ax1.annotate(f"{a/b:.2f}x", (x, (a+b)/2), ha="center", fontsize=9.5,
                     color="#374151", fontweight="bold",
                     bbox=dict(boxstyle="round,pad=0.25", fc="white", ec="#9ca3af", alpha=0.9))

ax1.set_ylabel("Sort time (s), median"); ax1.set_title("Cost", fontsize=11)
ax2.axhline(1.0, color="black", lw=0.9, alpha=0.6)
ax2.set_ylabel("Relative to that arm at 32MB")
ax2.set_title("Does more memory help?", fontsize=11)
for ax in (ax1, ax2):
    ax.set_xscale("log", base=2); ax.set_xticks(xs)
    ax.set_xticklabels([m.replace("MB", "") for m in M])
    ax.set_xlabel("Sort memory budget (MB)"); ax.grid(alpha=0.3, ls=":")
ax1.legend(frameon=False, fontsize=9.5)
ax1.margins(y=0.18); ax2.margins(y=0.12)
fig.suptitle("TPC-DS store_sales (28.8M rows), ORDER BY ss_sales_price\n"
             "One `?` in AsterixDB's own shipped schema costs stock 1.57-2.05x; auto-detection recovers it to within 3%",
             fontsize=11.5, y=1.0)
fig.tight_layout()
fig.savefig(DST, dpi=160, bbox_inches="tight")
print("wrote", DST)
