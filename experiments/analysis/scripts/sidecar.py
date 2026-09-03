#!/usr/bin/env python3
"""Sample AsterixDB's CPU and disk I/O while a query runs, in real units.

Reads kernel counters directly (/proc/PID/stat, /proc/PID/io) rather than sampling the whole
machine, so the numbers belong to the NC JVMs and not to anything else on a shared box. This is the
same data psutil exposes, minus the dependency.

  cpu_pct     percent of ONE core (200 = two cores saturated), from utime+stime jiffies
  read_MBps   MB/s actually fetched from the block device (read_bytes), not page-cache hits
  write_MBps  MB/s sent to the block device (write_bytes)

usage: sidecar.py <out.csv> [interval_s] [match]     stop with SIGTERM/SIGINT
"""
import os, sys, time, signal, glob

OUT = sys.argv[1]
INTERVAL = float(sys.argv[2]) if len(sys.argv) > 2 else 0.1
MATCH = sys.argv[3] if len(sys.argv) > 3 else "asterixnc"
HZ = os.sysconf("SC_CLK_TCK")

stop = False
def _stop(*_): 
    global stop; stop = True
signal.signal(signal.SIGTERM, _stop); signal.signal(signal.SIGINT, _stop)

def pids():
    out = []
    for d in glob.glob("/proc/[0-9]*"):
        try:
            with open(d + "/cmdline", "rb") as f:
                if MATCH.encode() in f.read():
                    out.append(d)
        except OSError:
            pass
    return out

def sample(procs):
    """Return (cpu_jiffies, read_bytes, write_bytes) summed over the matched processes."""
    cpu = rd = wr = 0
    for d in procs:
        try:
            # utime and stime are fields 14 and 15, but comm may contain spaces -> split after ')'
            st = open(d + "/stat").read()
            fields = st[st.rindex(")") + 2:].split()
            cpu += int(fields[11]) + int(fields[12])
        except (OSError, ValueError, IndexError):
            pass
        try:
            for line in open(d + "/io"):
                if line.startswith("read_bytes:"):  rd += int(line.split()[1])
                elif line.startswith("write_bytes:"): wr += int(line.split()[1])
        except OSError:
            pass
    return cpu, rd, wr

procs = pids()
if not procs:
    sys.stderr.write(f"sidecar: no process matching {MATCH!r}\n"); sys.exit(2)

t0 = time.time()
pc, pr, pw = sample(procs)
pt = t0
with open(OUT, "w", buffering=1) as f:
    f.write("elapsed_s,cpu_pct,read_MBps,write_MBps\n")
    while not stop:
        time.sleep(INTERVAL)
        now = time.time()
        c, r, w = sample(procs)
        dt = now - pt
        if dt > 0:
            f.write(f"{now-t0:.3f},{(c-pc)/HZ/dt*100:.1f},"
                    f"{(r-pr)/dt/1048576:.2f},{(w-pw)/dt/1048576:.2f}\n")
        pc, pr, pw, pt = c, r, w, now
