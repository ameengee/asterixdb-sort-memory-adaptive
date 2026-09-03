#!/usr/bin/env python3
"""Figure 1 — the good-neighbour claim, in real units.

Two stacked panels against wall-clock seconds:
  top    block-device I/O, MB/s   (read and write, summed)
  bottom CPU, % of one core

Two lines each: stock AsterixDB vs our bucketed sort. The argument is the SHAPE -- stock does its
I/O in a block that falls to zero and then spikes CPU; bucketed sustains both, because sort work is
interleaved with data arrival. This replaces `spreadPct`, which is a unit only we understand.

usage: make_neighbor_figure.py <stock.csv> <bucket.csv> <out.png> [summary.txt]
"""
import sys, csv, os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

stock_csv, bucket_csv, dst = sys.argv[1], sys.argv[2], sys.argv[3]
summary = sys.argv[4] if len(sys.argv) > 4 else None

def load(p):
    t, io, cpu = [], [], []
    with open(p) as f:
        for row in csv.DictReader(f):
            t.append(float(row["elapsed_s"]))
            io.append(float(row["read_MBps"]) + float(row["write_MBps"]))
            cpu.append(float(row["cpu_pct"]))
    return t, io, cpu

series = [("Stock AsterixDB", stock_csv, "#b91c1c", "-"),
          ("Ours (bucketed)", bucket_csv, "#1d4ed8", "-")]

fig, (ax_io, ax_cpu) = plt.subplots(2, 1, figsize=(11, 7), sharex=True)
for label, path, color, ls in series:
    if not os.path.exists(path):
        print(f"  missing {path}"); continue
    t, io, cpu = load(path)
    ax_io.plot(t, io, color=color, ls=ls, lw=1.4, label=label)
    ax_cpu.plot(t, cpu, color=color, ls=ls, lw=1.4, label=label)
    # integrate to report totals -- a shape argument is only worth making if the volumes are sane
    dt = (t[1] - t[0]) if len(t) > 1 else 0.1
    print(f"  {label:<18} {t[-1]:6.1f}s   I/O total {sum(io)*dt/1024:6.2f} GB   "
          f"peak {max(io):7.1f} MB/s   CPU peak {max(cpu):6.0f}%   CPU mean {sum(cpu)/len(cpu):5.0f}%")

ax_io.set_ylabel("Disk I/O (MB/s)")
ax_io.set_title("Sort operator: disk and CPU over the life of one query", fontsize=12)
ax_cpu.set_ylabel("CPU (% of one core)")
ax_cpu.set_xlabel("Time since query start (s)")
for ax in (ax_io, ax_cpu):
    ax.grid(alpha=.3, ls=":")
    ax.margins(x=0.01)
ax_io.legend(frameon=False, fontsize=10)
fig.tight_layout()
fig.savefig(dst, dpi=160)
print(f"  wrote {dst}")
if summary and os.path.exists(summary):
    print("\n  measured totals from the run:")
    for line in open(summary):
        print("   ", line.rstrip())
