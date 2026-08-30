#!/usr/bin/env python3
"""
Memory-adaptive sort -- NC log scraper.

Turns the sorter's log lines into CSVs that pair with the driver's per-query CSV.

  adaptive-sort:        one row per broker DECISION (grant-grow / victim-full /
                        victim-periodic-shrink / victim-periodic-spill / denied)
  adaptive-sort-run:    one row per RUN produced, with the config that produced it
                        (mergeFanIn, bucketTargetBytes) so a result is self-describing
  adaptive-sort-broker: the policy in effect, emitted once per operator construction

Usage:
  ./scrape_sort_logs.py --logs <CLUSTER>/logs --label e1-adaptive --outdir results
  ./scrape_sort_logs.py --logs <CLUSTER>/logs --label e3 --since-line-file results/e3.since

--since-line-file lets you isolate a single run from an accumulating log: pass the same path
before and after; it stores each log's line count so the next scrape starts where this one ended.
"""

import argparse
import csv
import glob
import json
import os
import re

DECISION_RE = re.compile(
    r"adaptive-sort:\s+(?P<reason>[a-z-]+)\s+budgetFrames=(?P<budget>\d+)\s+"
    r"\(min=(?P<min>\d+),\s*max=(?P<max>\d+)\)")

RUN_RE = re.compile(
    r"adaptive-sort-run:\s+framesLoaded=(?P<frames>\d+)\s+bytesUsed=(?P<bytes>\d+)\s+"
    r"budgetBytes=(?P<budgetBytes>-?\d+)\s+fillPct=(?P<fill>-?\d+)\s+tuples=(?P<tuples>\d+)"
    r"(?:\s+mergeFanIn=(?P<fanIn>\d+)\s+bucketTargetBytes=(?P<bucket>\d+))?")

BROKER_RE = re.compile(r"adaptive-sort-broker:\s+policy=(?P<policy>\S+)\s+impl=(?P<impl>\S+)")


def load_since(path):
    if path and os.path.exists(path):
        with open(path) as fh:
            return json.load(fh)
    return {}


def main():
    ap = argparse.ArgumentParser(description="Scrape adaptive-sort log lines into CSV")
    ap.add_argument("--logs", required=True, help="cluster logs directory")
    ap.add_argument("--label", required=True)
    ap.add_argument("--outdir", default="results")
    ap.add_argument("--pattern", default="nc-*.log")
    ap.add_argument("--since-line-file", default=None,
                    help="JSON file tracking per-log line offsets, to isolate one run")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    since = load_since(args.since_line_file)
    new_since = {}

    decisions, runs, brokers = [], [], []

    for path in sorted(glob.glob(os.path.join(args.logs, args.pattern))):
        node = os.path.basename(path)
        start = since.get(node, 0)
        with open(path, errors="replace") as fh:
            lines = fh.readlines()
        new_since[node] = len(lines)
        for lineno, line in enumerate(lines[start:], start=start + 1):
            m = DECISION_RE.search(line)
            if m:
                decisions.append({"node": node, "line": lineno, "reason": m.group("reason"),
                                  "budgetFrames": m.group("budget"),
                                  "minFrames": m.group("min"), "maxFrames": m.group("max")})
                continue
            m = RUN_RE.search(line)
            if m:
                runs.append({"node": node, "line": lineno,
                             "framesLoaded": m.group("frames"), "bytesUsed": m.group("bytes"),
                             "budgetBytes": m.group("budgetBytes"), "fillPct": m.group("fill"),
                             "tuples": m.group("tuples"),
                             "mergeFanIn": m.group("fanIn") or "",
                             "bucketTargetBytes": m.group("bucket") or ""})
                continue
            m = BROKER_RE.search(line)
            if m:
                brokers.append({"node": node, "line": lineno,
                                "policy": m.group("policy"), "impl": m.group("impl")})

    def write(name, rows, fields):
        path = os.path.join(args.outdir, f"{args.label}.{name}.csv")
        with open(path, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=fields)
            w.writeheader()
            w.writerows(rows)
        return path

    p1 = write("decisions", decisions,
               ["node", "line", "reason", "budgetFrames", "minFrames", "maxFrames"])
    p2 = write("runs", runs,
               ["node", "line", "framesLoaded", "bytesUsed", "budgetBytes", "fillPct",
                "tuples", "mergeFanIn", "bucketTargetBytes"])
    p3 = write("broker", brokers, ["node", "line", "policy", "impl"])

    if args.since_line_file:
        with open(args.since_line_file, "w") as fh:
            json.dump(new_since, fh)

    counts = {}
    for d in decisions:
        counts[d["reason"]] = counts.get(d["reason"], 0) + 1

    print(f"[scrape] decisions={len(decisions)} runs={len(runs)} -> {p1}, {p2}, {p3}")
    if counts:
        print("[scrape] decision mix: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    if brokers:
        policies = sorted({b["policy"] for b in brokers})
        print(f"[scrape] broker policy in effect: {policies}")
    else:
        print("[scrape] WARNING: no adaptive-sort-broker line found -- "
              "cannot confirm which policy ran")
    if runs:
        fan = sorted({r["mergeFanIn"] for r in runs if r["mergeFanIn"]})
        buckets = sorted({r["bucketTargetBytes"] for r in runs if r["bucketTargetBytes"]})
        print(f"[scrape] mergeFanIn seen: {fan or 'n/a'}   bucketTargetBytes seen: {buckets or 'n/a'}")


if __name__ == "__main__":
    main()
