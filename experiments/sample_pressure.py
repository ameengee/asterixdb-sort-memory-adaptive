#!/usr/bin/env python3
"""
Memory-adaptive sort -- CPU / disk / memory pressure sampler.

Samples the AsterixDB NC JVMs at a fixed interval and writes a time series. The point is to make
the *shape* of resource use visible, not just totals: stock AsterixDB accumulates frames (I/O
heavy, CPU idle) and then sorts everything at once (CPU spike, I/O idle), producing an
interlocking sawtooth. The incremental bucket sort is supposed to flatten it by doing sort work
as data arrives.

Per-process disk counters are only available on Linux (`/proc/<pid>/io`). On macOS the sampler
falls back to SYSTEM-WIDE disk counters and labels the mode in the CSV, so a local reading is
never silently mistaken for a per-process one.

Usage:
  ./sample_pressure.py --label e1-adaptive --duration 300 --interval 0.1
  ./sample_pressure.py --label profile --duration 60 --interval 0.05 --outdir results
"""

import argparse
import csv
import os
import sys
import time

try:
    import psutil
except ImportError:
    sys.exit("psutil required:  pip3 install psutil")

NC_HINTS = ("asterixnc", "NCDriver", "NCApplication", "hyracks.control.nc")
CC_HINTS = ("asterixcc", "CCDriver", "hyracks.control.cc")


def find_procs(hints):
    found = []
    for p in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            cmd = " ".join(p.info["cmdline"] or [])
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
        if any(h in cmd for h in hints):
            found.append(psutil.Process(p.info["pid"]))
    return found


def proc_io(proc):
    """Per-process (read_bytes, write_bytes), or None where unsupported (macOS)."""
    try:
        io = proc.io_counters()
        return io.read_bytes, io.write_bytes
    except (psutil.AccessDenied, AttributeError, NotImplementedError, psutil.NoSuchProcess):
        return None


def main():
    ap = argparse.ArgumentParser(description="Sample NC CPU/disk/memory pressure")
    ap.add_argument("--label", required=True)
    ap.add_argument("--outdir", default="results")
    ap.add_argument("--duration", type=float, default=300)
    ap.add_argument("--interval", type=float, default=0.1, help="seconds between samples")
    ap.add_argument("--include-cc", action="store_true", help="also sample the CC process")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    path = os.path.join(args.outdir, f"{args.label}.pressure.csv")

    procs = find_procs(NC_HINTS)
    if args.include_cc:
        procs += find_procs(CC_HINTS)
    if not procs:
        sys.exit("no AsterixDB NC processes found -- is the cluster running?")

    per_proc_io = proc_io(procs[0]) is not None
    io_mode = "per-process" if per_proc_io else "system-wide"
    print(f"[pressure] {len(procs)} process(es): {[p.pid for p in procs]}  io={io_mode}  "
          f"interval={args.interval}s duration={args.duration}s", flush=True)

    # Prime cpu_percent: the first call always returns 0.0 (it needs a prior reading to diff).
    for p in procs:
        try:
            p.cpu_percent(interval=None)
        except psutil.NoSuchProcess:
            pass

    prev_io = None
    if per_proc_io:
        prev_io = [0, 0]
        for p in procs:
            io = proc_io(p)
            if io:
                prev_io[0] += io[0]
                prev_io[1] += io[1]
    else:
        d = psutil.disk_io_counters()
        prev_io = [d.read_bytes, d.write_bytes]

    fields = ["epoch", "elapsedSec", "cpuPctTotal", "cpuPctPerProc", "rssMbTotal",
              "readBytesDelta", "writeBytesDelta", "readMBps", "writeMBps", "ioMode", "nProcs"]

    started = time.time()
    last = started
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        while True:
            time.sleep(args.interval)
            now = time.time()
            elapsed = now - started
            if elapsed >= args.duration:
                break
            dt = now - last
            last = now

            cpu_each, rss_total = [], 0
            alive = []
            for p in procs:
                try:
                    cpu_each.append(p.cpu_percent(interval=None))
                    rss_total += p.memory_info().rss
                    alive.append(p)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            procs = alive or procs

            if per_proc_io:
                cur = [0, 0]
                for p in procs:
                    io = proc_io(p)
                    if io:
                        cur[0] += io[0]
                        cur[1] += io[1]
            else:
                d = psutil.disk_io_counters()
                cur = [d.read_bytes, d.write_bytes]

            dr = max(0, cur[0] - prev_io[0])
            dw = max(0, cur[1] - prev_io[1])
            prev_io = cur

            w.writerow({
                "epoch": f"{now:.4f}",
                "elapsedSec": f"{elapsed:.4f}",
                "cpuPctTotal": f"{sum(cpu_each):.2f}",
                "cpuPctPerProc": "|".join(f"{c:.1f}" for c in cpu_each),
                "rssMbTotal": f"{rss_total / (1024 * 1024):.1f}",
                "readBytesDelta": dr,
                "writeBytesDelta": dw,
                "readMBps": f"{dr / dt / (1024 * 1024):.3f}" if dt > 0 else "",
                "writeMBps": f"{dw / dt / (1024 * 1024):.3f}" if dt > 0 else "",
                "ioMode": io_mode,
                "nProcs": len(procs),
            })
            fh.flush()

    print(f"[pressure] done -> {path}", flush=True)


if __name__ == "__main__":
    main()
