#!/usr/bin/env python3
"""
Publication figures for the memory-adaptive sort.

Reads the sorter's own 100ms instrumentation traces (adaptive-sort-series lines, produced under
-Dhyracks.sort.phaseLog=true) and produces two figures:

  fig1_io_cpu.{pdf,png}   When I/O happens vs when CPU happens. This is the central claim:
                          load-then-sort uses the two resources in disjoint phases; the bucketed
                          sorter overlaps them.
  fig2_memory.{pdf,png}   How much sort memory is held, and for how long.
  fig3_fanin.{pdf,png}    Cascade fan-in costs nothing in query time, so it can be chosen for
                          adaptivity rather than for speed.
  fig4_mergework.{pdf,png}  PROVISIONAL. Merge work is conserved, only relocated between the
                          cascade and the final merge. The underlying raw data was NOT saved to a
                          file before the instance was terminated -- FANIN_WORK below is
                          hand-transcribed from a terminal session and cannot be regenerated from
                          the repo. Re-run with output captured before using this in the paper.

One line per series. Each trace is a SINGLE partition: both partitions log run=0, so a naive
merge interleaves two traces into a sawtooth -- segments are split on a tRelMs reset.

The traces only cover the arrival phase (samples are emitted from insertFrame). Work that happens
after the last frame -- the single big sort, or the bucketed final merge -- is appended from the
per-run phase counters and is drawn with a dashed line so measured and appended parts stay
visually distinct.
"""

import re
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Post-arrival work, from the adaptive-sort-phase counters (seconds).
AFTER_LOAD = {
    "nobucket": {"label": "single sort after last frame", "dur": 13.99, "work": 13.99},
    "bucketed": {"label": "final merge after last frame", "dur": 2.91, "work": 2.91},
}
# Colours are SEMANTIC, not per-arm: I/O is always teal, CPU always orange, so the two panels of
# figure 1 differ only in shape. Figure 2 compares the arms directly, so there it uses arm colours.
C_IO, C_CPU = "#2F7D74", "#C2571A"
STYLE = {
    "nobucket": {"color": "#3B6EA5", "name": "load-then-sort (stock behaviour)"},
    "bucketed": {"color": "#C2571A", "name": "incremental bucket sort"},
}
BYTES_PER_MB = 1048576.0


def load_one_partition(path):
    """Return the first complete monotonic segment (one partition's trace)."""
    rows = [dict(re.findall(r"(\w+)=(-?\d+)", l)) for l in open(path)]
    segs, cur, last = [], [], -1
    for r in rows:
        t = int(r["tRelMs"])
        if t < last:
            segs.append(cur)
            cur = []
        cur.append(r)
        last = t
    segs.append(cur)
    segs = [s for s in segs if len(s) > 5]
    seg = max(segs, key=len)
    return {
        "t": [int(r["tRelMs"]) / 1000.0 for r in seg],
        "mem": [int(r["memUsed"]) / BYTES_PER_MB for r in seg],
        "work": [(int(r["cumSortNs"]) + int(r["cumCascadeNs"])) / 1e9 for r in seg],
        "frames": [int(r["frames"]) for r in seg],
    }


def trapz(x, y):
    return sum((x[i] - x[i - 1]) * (y[i] + y[i - 1]) / 2 for i in range(1, len(x)))


# Cascade fan-in sweep. Query times are 3 reps per setting (verified honored: the NC log echoed
# mergeFanIn=2,4,8,32). The work split comes from a separate instrumented run, so the two panels
# are different runs of the same configuration and are labelled as such.
FANIN_QUERY = {
    "2": [20.94, 21.19, 21.72],
    "4": [20.43, 20.93, 20.80],
    "8": [21.18, 21.21, 21.69],
    "32": [21.98, 20.57, 20.17],
    "none": [21.22, 21.14, 21.39],
}
# PROVISIONAL -- hand-transcribed, no saved raw data. See fig4 note above. Fan-in 4 is missing
# because the ad-hoc run that produced these skipped it.
FANIN_WORK = {  # (cascade s, final merge s)
    "2": (6.24, 2.91),
    "8": (4.52, 6.03),
    "32": (3.42, 5.41),
    "none": (0.00, 8.96),
}


def figure_fanin():
    """fig3: query time vs cascade fan-in. Raw data: results/ec2/logs/round2.log (FANIN lines)."""
    import statistics as st
    keys = ["2", "4", "8", "32", "none"]
    labels = ["2\n(eager)", "4", "8", "32", "none\n(lazy)"]
    x = list(range(len(keys)))
    fig, ax1 = plt.subplots(figsize=(5.6, 3.9))

    # min-max error bars rather than a line joining the medians: a connecting line would imply a
    # trend, when the point is that every setting overlaps every other.
    meds = [st.median(FANIN_QUERY[k]) for k in keys]
    lo = [st.median(FANIN_QUERY[k]) - min(FANIN_QUERY[k]) for k in keys]
    hi = [max(FANIN_QUERY[k]) - st.median(FANIN_QUERY[k]) for k in keys]
    grand = st.median([v for k in keys for v in FANIN_QUERY[k]])
    ax1.errorbar(x, meds, yerr=[lo, hi], fmt="o", ms=6, color="#3B6EA5",
                 ecolor="#8FA8BF", elinewidth=1.6, capsize=5, zorder=4,
                 label="median, min–max of 3 runs")
    ax1.axhline(grand, color="#3B6EA5", lw=1, ls="--", alpha=0.65, zorder=2,
                label=f"overall median {grand:.1f}s")
    ax1.set_xticks(x, labels)
    ax1.set_xlim(-0.5, len(keys) - 0.5)
    allv = [v for k in keys for v in FANIN_QUERY[k]]
    pad = (max(allv) - min(allv)) * 0.45
    ax1.set_ylim(min(allv) - pad, max(allv) + pad * 0.6)
    ax1.set_xlabel("cascade fan-in")
    ax1.set_ylabel("query time (seconds)")
    ax1.set_title("Cascade fan-in does not change query time", fontsize=11, loc="left")
    ax1.grid(True, color="#DDDDDD", lw=0.7)
    ax1.set_axisbelow(True)
    ax1.legend(fontsize=8, loc="lower center", framealpha=0.95, ncol=2)
    spread = max(max(v) - min(v) for v in FANIN_QUERY.values())
    ax1.annotate(f"between settings: {max(meds)-min(meds):.2f}s\n"
                 f"within one setting: up to {spread:.2f}s",
                 xy=(0.97, 0.955), xycoords="axes fraction", fontsize=8,
                 ha="right", va="top", color="#555555")
    fig.text(0.5, 0.015,
             "3 runs per setting; fan-in confirmed in the NC log each time.\n"
             "10M rows, 512 MB sort memory.",
             ha="center", fontsize=7.5, color="#555555", linespacing=1.5)
    fig.tight_layout(rect=[0, 0.10, 1, 1])
    for ext in ("pdf", "png"):
        fig.savefig(f"figures/fig3_fanin.{ext}", dpi=200)
    plt.close(fig)
    print(f"\nfan-in query time: between settings {max(meds)-min(meds):.2f}s "
          f"vs up to {spread:.2f}s within one setting")


def figure_mergework():
    """fig4: where merge work goes. PROVISIONAL -- see FANIN_WORK note; no saved raw data."""
    wk = list(FANIN_WORK)
    wx = list(range(len(wk)))
    casc = [FANIN_WORK[k][0] for k in wk]
    fin = [FANIN_WORK[k][1] for k in wk]
    fig, ax2 = plt.subplots(figsize=(5.6, 3.9))
    ax2.bar(wx, casc, width=0.6, color="#C2571A", label="cascade merges (during arrival)")
    ax2.bar(wx, fin, width=0.6, bottom=casc, color="#E9B384", label="final merge (at flush)")
    for i, (c, f) in enumerate(zip(casc, fin)):
        ax2.text(i, c + f + 0.25, f"{c+f:.1f}s", ha="center", fontsize=8, color="#333333")
    ax2.set_xticks(wx, [{"2": "2\n(eager)", "8": "8", "32": "32",
                         "none": "none\n(lazy)"}[k] for k in wk])
    ax2.set_xlabel("cascade fan-in")
    ax2.set_ylabel("merge work (seconds)")
    ax2.set_title("Merge work is conserved, only relocated", fontsize=11, loc="left")
    ax2.set_ylim(0, max(c + f for c, f in zip(casc, fin)) * 1.45)
    ax2.grid(True, axis="y", color="#DDDDDD", lw=0.7)
    ax2.set_axisbelow(True)
    ax2.legend(fontsize=8, loc="upper center", framealpha=0.95)
    fig.text(0.5, 0.015,
             "PROVISIONAL: one instrumented run per setting, transcribed from a terminal session;\n"
             "raw data not saved. Re-run with captured output before publishing.",
             ha="center", fontsize=7.5, color="#8A4B12", linespacing=1.5)
    fig.tight_layout(rect=[0, 0.10, 1, 1])
    for ext in ("pdf", "png"):
        fig.savefig(f"figures/fig4_mergework.{ext}", dpi=200)
    plt.close(fig)


def main():
    d = {k: load_one_partition(f"results/ec2/series3-{k}.txt") for k in STYLE}

    # ---------------- Figure 1: when is I/O busy, when is CPU busy ----------------
    fig, axes = plt.subplots(2, 1, figsize=(7.2, 5.8), sharex=True)
    for ax, key in zip(axes, ("nobucket", "bucketed")):
        s, st = d[key], STYLE[key]
        a = AFTER_LOAD[key]
        t_end = s["t"][-1]
        io_pct = [100.0 * f / s["frames"][-1] for f in s["frames"]]
        total_work = s["work"][-1] + a["work"]
        cpu_pct = [100.0 * w / total_work for w in s["work"]]

        l1, = ax.plot(s["t"], io_pct, color=C_IO, lw=2,
                      label="I/O  — data read into the sorter")
        l2, = ax.plot(s["t"], cpu_pct, color=C_CPU, lw=2,
                      label="CPU — sort work completed")
        ax.plot([t_end, t_end + a["dur"]], [100, 100], color=C_IO, lw=2, ls="--")
        ax.plot([t_end, t_end + a["dur"]], [cpu_pct[-1], 100.0], color=C_CPU, lw=2, ls="--")
        ax.axvline(t_end, color="#888888", lw=0.9, ls=":")
        ax.text(t_end - 0.25, 50, "last frame arrives", rotation=90, fontsize=7.5,
                color="#666666", ha="right", va="center")

        ax.set_title(st["name"], fontsize=10, loc="left", pad=5)
        ax.set_ylabel("cumulative, % of run total")
        ax.set_ylim(-4, 108)
        ax.set_yticks([0, 25, 50, 75, 100])
        ax.grid(True, color="#DDDDDD", lw=0.7)
        ax.set_axisbelow(True)
    axes[-1].set_xlabel("time since the run's first frame (seconds)")
    fig.legend(handles=[l1, l2], loc="upper center", ncol=2, fontsize=9,
               frameon=False, bbox_to_anchor=(0.5, 0.945))
    fig.suptitle("When the sort operator uses disk vs CPU", fontsize=12.5, y=0.995)
    fig.text(0.5, 0.015,
             "Solid = measured during arrival.  Dashed = work after the last frame, from per-run "
             "phase counters.\n10M rows, 512 MB sort memory, one partition of two.",
             ha="center", fontsize=7.5, color="#555555", linespacing=1.5)
    fig.tight_layout(rect=[0, 0.065, 1, 0.925])
    for ext in ("pdf", "png"):
        fig.savefig(f"figures/fig1_io_cpu.{ext}", dpi=200)
    plt.close(fig)

    # ---------------- Figure 2: memory held over time ----------------
    fig, ax = plt.subplots(figsize=(7.2, 4.2))
    stats = {}
    for key in ("nobucket", "bucketed"):
        s, st = d[key], STYLE[key]
        a = AFTER_LOAD[key]
        t_end, peak = s["t"][-1], s["mem"][-1]
        # Memory is HELD at its peak for the whole post-arrival phase, then released at flush.
        t_full = s["t"] + [t_end + a["dur"]]
        m_full = s["mem"] + [peak]
        ax.plot(s["t"], s["mem"], color=st["color"], lw=2, label=st["name"])
        ax.plot([t_end, t_end + a["dur"]], [peak, peak], color=st["color"], lw=2, ls="--")
        ax.plot([t_end + a["dur"]] * 2, [peak, 0], color=st["color"], lw=1.2, ls="--", alpha=0.55)
        area = trapz(t_full, m_full)
        stats[key] = (area, t_full[-1], area / t_full[-1], peak)
    for key in ("nobucket", "bucketed"):
        area, dur, mean, peak = stats[key]
        ax.axhline(mean, color=STYLE[key]["color"], lw=0.9, ls="-.", alpha=0.6)
    n, b = stats["nobucket"], stats["bucketed"]
    ax.annotate(f"mean occupancy {n[2]:.0f} MB", xy=(n[1] * 0.52, n[2] + 5),
                fontsize=8, color=STYLE["nobucket"]["color"])
    ax.annotate(f"mean occupancy {b[2]:.0f} MB", xy=(b[1] * 0.52, b[2] - 14),
                fontsize=8, color=STYLE["bucketed"]["color"])
    ax.set_xlabel("time since first frame of the run (seconds)")
    ax.set_ylabel("sort memory held (MB)")
    ax.set_title("Sort memory held over the life of a run", fontsize=12, loc="left")
    ax.grid(True, color="#DDDDDD", lw=0.7)
    ax.set_axisbelow(True)
    ax.set_ylim(0, max(n[3], b[3]) * 1.18)
    ax.legend(fontsize=8, loc="lower right", framealpha=0.95)
    fig.text(0.5, 0.015,
             "Solid = measured during arrival.  Dashed = peak held while the remaining sort/merge "
             "runs, then released.\nDash-dot = time-averaged occupancy over the run's life.",
             ha="center", fontsize=7.5, color="#555555", linespacing=1.5)
    fig.tight_layout(rect=[0, 0.10, 1, 1])
    for ext in ("pdf", "png"):
        fig.savefig(f"figures/fig2_memory.{ext}", dpi=200)
    plt.close(fig)

    print(f"{'arm':28}{'peak MB':>9}{'duration s':>12}{'mean MB':>10}{'MB*s':>10}")
    for key in ("nobucket", "bucketed"):
        area, dur, mean, peak = stats[key]
        print(f"{STYLE[key]['name']:28}{peak:>9.0f}{dur:>12.1f}{mean:>10.0f}{area:>10.0f}")
    print(f"\nmean occupancy: bucketed is {100*(1-b[2]/n[2]):.0f}% lower than load-then-sort")
    figure_fanin()
    figure_mergework()
    print("wrote figures/fig1_io_cpu, fig2_memory, fig3_fanin, fig4_mergework (.pdf and .png)")


if __name__ == "__main__":
    main()
