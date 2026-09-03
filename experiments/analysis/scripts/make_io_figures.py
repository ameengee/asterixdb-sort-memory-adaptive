#!/usr/bin/env python3
"""Figures for the overnight I/O-pressure suite -> analysis/figures_io/

Every panel here was measured with a 44GB balloon and a cache drop before each query, so run files
actually reach disk. That matters: earlier cache-resident runs could not show a memory benefit at
all, because a single-pass merge moves the same bytes regardless of run count.

Input:  times.txt  lines "<kind> <dataset> <arm> <mem> <seconds>"
        spread.txt lines "SPREAD <dataset> <arm> spreadPct=N sortEvents=M"
"""
import sys, os, statistics as st
from collections import defaultdict
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

SRC = sys.argv[1] if len(sys.argv) > 1 else "../data/overnight_times.txt"
SPREAD = sys.argv[2] if len(sys.argv) > 2 else "../data/overnight_spread.txt"
OUT = sys.argv[3] if len(sys.argv) > 3 else "../figures_io"
os.makedirs(OUT, exist_ok=True)

def mb(m): return int(m.replace("MB", ""))

rows = defaultdict(list)
for line in open(SRC):
    f = line.split()
    if len(f) == 5:
        rows[(f[0], f[1], f[2], f[3])].append(float(f[4]))

DSLABEL = {"test": "600MB synthetic, single type",
           "mixbig": "600MB synthetic, multi-type",
           "tpcds": "TPC-DS store_sales (28.8M rows)"}
ARM = {"stock":    ("Stock AsterixDB",              "#b91c1c", "o", "-"),
       "nobucket": ("Ours: type fix, no bucketing", "#0f766e", "s", "--"),
       "bucket":   ("Ours: type fix + bucketing",   "#1d4ed8", "D", "-")}

def series(kind, ds, arm):
    got = [(mb(m), st.median(v)) for (k, d, a, m), v in rows.items()
           if k == kind and d == ds and a == arm]
    return sorted(got)

def label_points(ax, xs, ys, color, rank, total):
    for x, y in zip(xs, ys):
        dy = -15 if rank == 0 else 8 + 12 * (rank - 1)
        ax.annotate(f"{y:.1f}", (x, y), textcoords="offset points", xytext=(0, dy),
                    ha="center", fontsize=7, color=color,
                    bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.75))

# ---- 1..3  U-curve / memory benefit / no-harm, one figure per dataset ----
for ds, dslabel in DSLABEL.items():
    present = [(a, series("S", ds, a)) for a in ARM if series("S", ds, a)]
    if not present:
        continue
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5.2))
    ordered = sorted(present, key=lambda t: st.median([y for _, y in t[1]]))
    for rank, (a, s) in enumerate(present):
        xs = [x for x, _ in s]; ys = [y for _, y in s]
        lbl, color, marker, ls = ARM[a]
        ax1.plot(xs, ys, label=lbl, color=color, marker=marker, ls=ls, lw=2, ms=6)
        ax2.plot(xs, [y / ys[0] for y in ys], label=lbl, color=color, marker=marker, ls=ls, lw=2, ms=6)
        label_points(ax1, xs, ys, color, rank, len(present))
        print(f"  {ds:<8} {a:<9} " + " ".join(f"{x}MB:{y:.1f}" for x, y in s)
              + f"   slope={(ys[-1]/ys[0]-1)*100:+.1f}%")
    ticks = sorted({x for _, s_ in present for x, _ in s_})
    for ax, ylab, title in ((ax1, "Sort time (s), median", "Cost"),
                            (ax2, "Relative to that arm's smallest budget",
                             "Does more memory help?")):
        ax.set_xscale("log", base=2)
        # label the ACTUAL budgets; matplotlib's default log ticks render as 2^n and hide which
        # budgets were measured
        ax.set_xticks(ticks); ax.set_xticklabels([str(t) for t in ticks])
        ax.minorticks_off()
        ax.set_xlabel("Sort memory budget (MB)")
        ax.set_ylabel(ylab); ax.set_title(title, fontsize=11); ax.grid(alpha=.3, ls=":")
    ax2.axhline(1.0, color="black", lw=.9, alpha=.6)
    ax1.legend(frameon=False, fontsize=9); ax1.margins(y=.18)
    fig.suptitle(f"{dslabel} — under I/O pressure (44GB balloon, cold cache per query)",
                 fontsize=12, y=1.0)
    fig.tight_layout(); fig.savefig(f"{OUT}/ucurve_{ds}.png", dpi=160, bbox_inches="tight")
    plt.close(fig); print(f"  wrote ucurve_{ds}.png")

# ---- 4  bucketing's marginal cost: same key, bucketing on vs off ----
fig, ax = plt.subplots(figsize=(8.4, 5.0))
for ds, color in (("test", "#1d4ed8"), ("mixbig", "#b45309"), ("tpcds", "#0f766e")):
    nb = dict(series("S", ds, "nobucket")); bk = dict(series("S", ds, "bucket"))
    common = sorted(set(nb) & set(bk))
    if not common: continue
    ys = [(bk[x] / nb[x] - 1) * 100 for x in common]
    ax.plot(common, ys, marker="o", lw=2, ms=6, color=color, label=DSLABEL[ds])
    for x, y in zip(common, ys):
        ax.annotate(f"{y:+.1f}%", (x, y), textcoords="offset points", xytext=(0, 8),
                    ha="center", fontsize=7, color=color)
    print(f"  bucketing cost {ds}: " + " ".join(f"{x}MB:{y:+.1f}%" for x, y in zip(common, ys)))
ax.axhline(0, color="black", lw=1)
ax.set_xscale("log", base=2)
allx = sorted({x for ds in DSLABEL for x in dict(series("S", ds, "bucket"))})
ax.set_xticks(allx); ax.set_xticklabels([str(t) for t in allx]); ax.minorticks_off()
ax.set_xlabel("Sort memory budget (MB)")
ax.set_ylabel("Bucketing's effect (%)   negative = faster")
ax.set_title("Does bucketing do harm? (same key, bucketing on vs off)", fontsize=12)
ax.grid(alpha=.3, ls=":"); ax.legend(frameon=False, fontsize=9)
fig.tight_layout(); fig.savefig(f"{OUT}/noharm.png", dpi=160); plt.close(fig)
print("  wrote noharm.png")

# ---- 5  neighbor property ----
sp = defaultdict(list); ev = defaultdict(list)
if os.path.exists(SPREAD):
    import re
    for line in open(SPREAD):
        f = line.split()
        if len(f) < 4 or f[0] != "SPREAD": continue
        m1 = re.search(r"spreadPct=(\d+)", line); m2 = re.search(r"sortEvents=(\d+)", line)
        if m1: sp[(f[1], f[2])].append(int(m1.group(1)))
        if m2: ev[(f[1], f[2])].append(int(m2.group(1)))
if sp:
    dss = sorted({k[0] for k in sp}); arms = ["flat", "bucketed"]
    fig, ax = plt.subplots(figsize=(8.4, 5.0))
    w = 0.35
    for i, a in enumerate(arms):
        vals = [st.median(sp.get((d, a), [0])) for d in dss]
        xs = [j + (i - .5) * w for j in range(len(dss))]
        ax.bar(xs, vals, w, label={"flat": "Load-then-sort (no bucketing)",
                                   "bucketed": "Bucketed (ours)"}[a],
               color="#9ca3af" if a == "flat" else "#1d4ed8")
        for x, v, d in zip(xs, vals, dss):
            e = st.median(ev.get((d, a), [0]))
            ax.annotate(f"{v:.0f}%\n{e:.0f} sort events", (x, v), textcoords="offset points",
                        xytext=(0, 5), ha="center", fontsize=8)
        print(f"  neighbor {a}: " + " ".join(f"{d}={st.median(sp.get((d,a),[0])):.0f}%" for d in dss))
    ax.set_xticks(range(len(dss))); ax.set_xticklabels([DSLABEL.get(d, d) for d in dss], fontsize=8)
    ax.set_ylabel("spreadPct — % of the run's life spent sorting")
    ax.set_title("Good neighbour: is sort work spread across data arrival?", fontsize=12)
    ax.legend(frameon=False, fontsize=9); ax.grid(alpha=.3, ls=":", axis="y"); ax.margins(y=.2)
    fig.tight_layout(); fig.savefig(f"{OUT}/neighbor.png", dpi=160); plt.close(fig)
    print("  wrote neighbor.png")

# ---- 6  reclaim above vs below the single-pass threshold ----
pairs = [("test", "64MB", "8MB"), ("tpcds", "128MB", "16MB")]
fig, ax = plt.subplots(figsize=(8.4, 5.0))
labels, costs, colors = [], [], []
for ds, hi, lo in pairs:
    for tag, where, c in (("hi", "above threshold", "#1d4ed8"), ("lo", "below threshold", "#b91c1c")):
        base = [v for (k, d, a, m), v in rows.items() if k == "R" and d == ds and a == f"none-{tag}"]
        recl = [v for (k, d, a, m), v in rows.items() if k == "R" and d == ds and a == f"reclaim-{tag}"]
        if not base or not recl: continue
        b = st.median([x for lst in base for x in lst]); r = st.median([x for lst in recl for x in lst])
        labels.append(f"{ds}\n{where}"); costs.append((r / b - 1) * 100); colors.append(c)
        print(f"  reclaim {ds} {where}: {b:.1f}s -> {r:.1f}s = {(r/b-1)*100:+.1f}%")
if costs:
    ax.bar(range(len(costs)), costs, color=colors)
    for i, c in enumerate(costs):
        ax.annotate(f"{c:+.1f}%", (i, c), textcoords="offset points", xytext=(0, 5 if c >= 0 else -14),
                    ha="center", fontsize=9)
    ax.axhline(0, color="black", lw=1)
    ax.set_xticks(range(len(labels))); ax.set_xticklabels(labels, fontsize=8)
    ax.set_ylabel("Cost of surrendering 50% of the budget (%)")
    ax.set_title("Releasing memory: free above the single-pass threshold, a merge pass below it",
                 fontsize=11)
    ax.grid(alpha=.3, ls=":", axis="y")
    fig.tight_layout(); fig.savefig(f"{OUT}/reclaim.png", dpi=160); plt.close(fig)
    print("  wrote reclaim.png")
print(f"\nfigures in {OUT}/")
