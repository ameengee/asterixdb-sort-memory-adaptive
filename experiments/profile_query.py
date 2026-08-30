#!/usr/bin/env python3
"""
Memory-adaptive sort -- single-query resource profile ("the sawtooth figure").

Runs ONE sort query while sampling CPU and disk at high frequency, and reports the shape of
resource use over that query's lifetime. This is the figure that shows the *mechanism*:

  stock     accumulate frames (I/O busy, CPU idle) -> then sort everything (CPU spike, I/O idle)
            -> then write the run.  Repeated per run: an interlocking sawtooth.
  adaptive  sort each cache-sized bucket as it fills, so CPU work is spread across arrival and
            the spike is flattened.

Aggregating many short queries blurs this; profiling one long query at 20-50ms resolution does
not. Use a LOW --sort-memory to force many runs, which makes the pattern repeat and stand out.

Usage:
  ./profile_query.py --label stock-profile --sort-memory 1MB --interval 0.02
  ./profile_query.py --label adaptive-profile --sort-memory 1MB --interval 0.02 --projection r
"""

import argparse
import csv
import json
import os
import statistics as st
import sys
import threading
import time
import urllib.parse
import urllib.request

try:
    import psutil
except ImportError:
    sys.exit("psutil required:  pip3 install psutil")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sample_pressure import find_procs, proc_io, NC_HINTS  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", default="http://127.0.0.1:19002/query/service")
    ap.add_argument("--label", required=True)
    ap.add_argument("--outdir", default="results")
    ap.add_argument("--dataverse", default="test")
    ap.add_argument("--dataset", default="ds")
    ap.add_argument("--sort-key", default="k")
    ap.add_argument("--projection", default="r")
    ap.add_argument("--sort-memory", default="1MB")
    ap.add_argument("--interval", type=float, default=0.02)
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--lead-in", type=float, default=1.0,
                    help="seconds of idle baseline sampled before the query starts")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    out = os.path.join(args.outdir, f"{args.label}.profile.csv")

    procs = find_procs(NC_HINTS)
    if not procs:
        sys.exit("no NC processes found -- is the cluster running?")
    per_proc_io = proc_io(procs[0]) is not None

    stmt = (f"USE {args.dataverse}; SET `compiler.sortmemory` \"{args.sort_memory}\"; "
            f"SELECT VALUE {args.projection} FROM {args.dataset} AS r ORDER BY r.{args.sort_key};")

    def run_query():
        data = urllib.parse.urlencode({"statement": stmt}).encode()
        with urllib.request.urlopen(args.endpoint, data=data, timeout=7200) as resp:
            return json.load(resp)

    for i in range(args.warmup):
        t0 = time.time()
        run_query()
        print(f"[profile] warmup {i + 1}/{args.warmup}: {time.time() - t0:.2f}s", flush=True)

    samples = []
    stop = threading.Event()
    state = {}

    def sampler():
        for p in procs:
            try:
                p.cpu_percent(interval=None)
            except psutil.NoSuchProcess:
                pass
        if per_proc_io:
            prev = [sum(x) for x in zip(*[proc_io(p) or (0, 0) for p in procs])]
        else:
            d = psutil.disk_io_counters()
            prev = [d.read_bytes, d.write_bytes]
        last = time.time()
        while not stop.is_set():
            time.sleep(args.interval)
            now = time.time()
            dt = now - last
            last = now
            cpu = 0.0
            for p in procs:
                try:
                    cpu += p.cpu_percent(interval=None)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            if per_proc_io:
                cur = [sum(x) for x in zip(*[proc_io(p) or (0, 0) for p in procs])]
            else:
                d = psutil.disk_io_counters()
                cur = [d.read_bytes, d.write_bytes]
            dr, dw = max(0, cur[0] - prev[0]), max(0, cur[1] - prev[1])
            prev = cur
            samples.append({
                "epoch": now, "cpuPct": cpu,
                "readMBps": dr / dt / (1024 ** 2) if dt > 0 else 0,
                "writeMBps": dw / dt / (1024 ** 2) if dt > 0 else 0,
            })

    t = threading.Thread(target=sampler, daemon=True)
    t.start()
    time.sleep(args.lead_in)  # idle baseline, so the query's onset is visible in the trace

    q_start = time.time()
    payload = run_query()
    q_end = time.time()
    state["ok"] = payload.get("status") == "success"

    time.sleep(args.lead_in)
    stop.set()
    t.join(timeout=5)

    with open(out, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["epoch", "tRelSec", "phase", "cpuPct",
                                           "readMBps", "writeMBps"])
        w.writeheader()
        for s in samples:
            rel = s["epoch"] - q_start
            phase = "query" if q_start <= s["epoch"] <= q_end else "idle"
            w.writerow({"epoch": f"{s['epoch']:.4f}", "tRelSec": f"{rel:.4f}", "phase": phase,
                        "cpuPct": f"{s['cpuPct']:.2f}",
                        "readMBps": f"{s['readMBps']:.4f}",
                        "writeMBps": f"{s['writeMBps']:.4f}"})

    during = [s for s in samples if q_start <= s["epoch"] <= q_end]
    cpu = [s["cpuPct"] for s in during]
    wr = [s["writeMBps"] for s in during]

    def cv(xs):
        if len(xs) < 2 or st.mean(xs) == 0:
            return float("nan")
        return st.pstdev(xs) / st.mean(xs)

    print(f"\n[profile] {args.label}: query {q_end - q_start:.2f}s, ok={state['ok']}, "
          f"{len(during)} samples @ {args.interval * 1000:.0f}ms  io={'per-proc' if per_proc_io else 'system'}")
    if cpu:
        print(f"[profile]   CPU%%  mean={st.mean(cpu):7.1f}  max={max(cpu):7.1f}  cv={cv(cpu):.3f}")
        print(f"[profile]   wrMBps mean={st.mean(wr):7.2f}  max={max(wr):7.2f}  cv={cv(wr):.3f}")
        # Coarse ASCII shape so the pattern is visible without plotting.
        buckets = 60
        if during:
            step = max(1, len(cpu) // buckets)
            coarse = [st.mean(cpu[i:i + step]) for i in range(0, len(cpu), step)][:buckets]
            hi = max(coarse) or 1
            print("\n[profile]   CPU shape over the query (each column ~"
                  f"{(q_end - q_start) / max(1, len(coarse)):.2f}s):")
            for row in range(8, 0, -1):
                thresh = hi * row / 8
                print("            |" + "".join("#" if c >= thresh else " " for c in coarse))
            print("            +" + "-" * len(coarse))
    print(f"[profile] -> {out}")


if __name__ == "__main__":
    main()
