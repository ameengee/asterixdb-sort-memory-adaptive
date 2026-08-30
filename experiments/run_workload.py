#!/usr/bin/env python3
"""
Memory-adaptive sort -- workload driver.

Issues a stream of sort queries against a running AsterixDB instance, optionally stepping
`compiler.sortmemory` on a schedule (the STOCK arm's only way to receive more memory), and
writes one CSV row per query plus a JSON run-manifest.

The manifest records the exact configuration so a result is never orphaned from the settings
that produced it -- see paper_experiment_plan.md section 5.

Usage
-----
  # E1 no-harm arm (broker inert, fixed memory)
  ./run_workload.py --label e1-adaptive --duration 600 --sort-memory 8MB

  # E2 grow arm: raise sort memory 10% every 30s
  ./run_workload.py --label e2-stock --duration 1800 --sort-memory 8MB \
      --grow-pct 10 --grow-every 30

  # verify sortedness on every query (slower; use for a short validation run)
  ./run_workload.py --label validate --duration 60 --verify

Notes
-----
* --verify downloads and checks full results (sorted + no tuple loss). Leave it OFF for timing
  runs: the transfer and check would dominate the measurement.
* Memory stepping applies to queries that START after the step, which is exactly the stock
  behavior we are contrasting against mid-flight adaptation.
"""

import argparse
import csv
import json
import os
import sys
import time
import urllib.parse
import urllib.request

DEFAULT_ENDPOINT = "http://127.0.0.1:19002/query/service"


def run_query(endpoint, statement, timeout, want_results):
    """POST one SQL++ statement. Returns (ok, metrics_dict, results_or_None, error_text)."""
    data = urllib.parse.urlencode({"statement": statement}).encode()
    started = time.time()
    try:
        with urllib.request.urlopen(endpoint, data=data, timeout=timeout) as resp:
            payload = json.load(resp)
    except Exception as exc:  # noqa: BLE001 - driver should survive a bad query
        return False, {"clientElapsed": time.time() - started}, None, str(exc)
    client_elapsed = time.time() - started
    metrics = payload.get("metrics", {})
    metrics["clientElapsed"] = client_elapsed
    ok = payload.get("status") == "success"
    results = payload.get("results") if want_results else None
    err = "" if ok else json.dumps(payload.get("errors", []))[:500]
    return ok, metrics, results, err


def parse_ms(value):
    """AsterixDB reports durations as strings like '1.305081s' or '287.259ms'."""
    if value is None:
        return ""
    text = str(value).strip()
    try:
        if text.endswith("ms"):
            return float(text[:-2])
        if text.endswith("s"):
            return float(text[:-1]) * 1000.0
        return float(text)
    except ValueError:
        return ""


def verify_sorted(results, key):
    """Returns (is_sorted, inversions, distinct_ids, rows). Guards against tuple loss."""
    if not results:
        return True, 0, 0, 0
    if isinstance(results[0], dict):
        keys = [r.get(key) for r in results]
        ids = {r.get("id") for r in results if "id" in r}
    else:
        keys = results
        ids = set()
    inversions = sum(1 for i in range(1, len(keys))
                     if keys[i - 1] is not None and keys[i] is not None and keys[i - 1] > keys[i])
    return inversions == 0, inversions, len(ids), len(results)


def main():
    ap = argparse.ArgumentParser(description="Memory-adaptive sort workload driver")
    ap.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    ap.add_argument("--label", required=True, help="run name; used for output filenames")
    ap.add_argument("--outdir", default="results")
    ap.add_argument("--duration", type=float, default=300, help="seconds to keep issuing queries")
    ap.add_argument("--max-queries", type=int, default=0, help="0 = unlimited (bounded by --duration)")
    ap.add_argument("--dataverse", default="test")
    ap.add_argument("--dataset", default="ds")
    ap.add_argument("--sort-key", default="k")
    ap.add_argument("--sort-memory", default="8MB", help="initial compiler.sortmemory")
    ap.add_argument("--projection", default="r", help="'r' for whole record, or e.g. 'r.k'")
    ap.add_argument("--grow-pct", type=float, default=0.0, help="raise sort memory by this %% each step")
    ap.add_argument("--grow-every", type=float, default=0.0, help="seconds between memory steps")
    ap.add_argument("--grow-cap-mb", type=float, default=0.0, help="0 = uncapped")
    ap.add_argument("--think-time", type=float, default=0.0, help="seconds to sleep between queries")
    ap.add_argument("--timeout", type=float, default=1800)
    ap.add_argument("--verify", action="store_true", help="check sortedness of every query")
    ap.add_argument("--warmup", type=int, default=1, help="queries to discard before recording")
    ap.add_argument("--note", default="", help="free-text note stored in the manifest")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    csv_path = os.path.join(args.outdir, f"{args.label}.csv")
    manifest_path = os.path.join(args.outdir, f"{args.label}.manifest.json")

    def mem_mb(spec):
        s = spec.strip().upper()
        if s.endswith("MB"):
            return float(s[:-2])
        if s.endswith("KB"):
            return float(s[:-2]) / 1024.0
        if s.endswith("GB"):
            return float(s[:-2]) * 1024.0
        return float(s)

    current_mb = mem_mb(args.sort_memory)
    initial_mb = current_mb

    statement_tmpl = (
        "USE {dv}; SET `compiler.sortmemory` \"{mem}MB\"; "
        "SELECT VALUE {proj} FROM {ds} AS r ORDER BY r.{key};"
    )

    manifest = {
        "label": args.label,
        "startedEpoch": time.time(),
        "startedIso": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "args": vars(args),
        "initialSortMemoryMB": initial_mb,
        # Recorded so a CSV is never orphaned from the JVM-side configuration that produced it.
        "brokerEnv": {k: v for k, v in os.environ.items() if k.startswith("SORT_EXP_")},
    }
    with open(manifest_path, "w") as fh:
        json.dump(manifest, fh, indent=2)

    fields = [
        "seq", "wallStartEpoch", "elapsedSinceStartSec", "sortMemoryMB", "ok",
        "clientElapsedMs", "elapsedTimeMs", "executionTimeMs", "compileTimeMs",
        "queueWaitTimeMs", "resultCount", "processedObjects", "memoryStepIndex",
        "verified", "inversions", "distinctIds", "rows", "error",
    ]

    started = time.time()
    next_grow = started + args.grow_every if args.grow_every > 0 else float("inf")
    step_index = 0
    seq = 0
    recorded = 0

    print(f"[driver] label={args.label} duration={args.duration}s "
          f"sortMemory={current_mb}MB grow={args.grow_pct}%/{args.grow_every}s", flush=True)

    with open(csv_path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()

        while True:
            now = time.time()
            if now - started >= args.duration:
                break
            if args.max_queries and recorded >= args.max_queries:
                break

            if now >= next_grow:
                grown = current_mb * (1.0 + args.grow_pct / 100.0)
                if args.grow_cap_mb > 0:
                    grown = min(grown, args.grow_cap_mb)
                if grown != current_mb:
                    current_mb = grown
                    step_index += 1
                    print(f"[driver] t={now - started:7.1f}s memory step {step_index} "
                          f"-> {current_mb:.2f}MB", flush=True)
                next_grow = now + args.grow_every

            stmt = statement_tmpl.format(dv=args.dataverse, ds=args.dataset, key=args.sort_key,
                                         mem=f"{current_mb:.4f}".rstrip("0").rstrip("."),
                                         proj=args.projection)
            seq += 1
            q_start = time.time()
            ok, metrics, results, err = run_query(args.endpoint, stmt, args.timeout, args.verify)

            if seq <= args.warmup:
                print(f"[driver] warmup {seq}/{args.warmup} ok={ok} "
                      f"{metrics.get('clientElapsed', 0):.2f}s", flush=True)
                continue

            verified = inversions = distinct_ids = rows = ""
            if args.verify and ok:
                is_sorted, inversions, distinct_ids, rows = verify_sorted(results, args.sort_key)
                verified = is_sorted
                if not is_sorted:
                    print(f"[driver] !! SORT ORDER VIOLATION on seq={seq} "
                          f"inversions={inversions}", file=sys.stderr, flush=True)

            writer.writerow({
                "seq": seq,
                "wallStartEpoch": f"{q_start:.3f}",
                "elapsedSinceStartSec": f"{q_start - started:.3f}",
                "sortMemoryMB": f"{current_mb:.4f}",
                "ok": ok,
                "clientElapsedMs": f"{metrics.get('clientElapsed', 0) * 1000:.3f}",
                "elapsedTimeMs": parse_ms(metrics.get("elapsedTime")),
                "executionTimeMs": parse_ms(metrics.get("executionTime")),
                "compileTimeMs": parse_ms(metrics.get("compileTime")),
                "queueWaitTimeMs": parse_ms(metrics.get("queueWaitTime")),
                "resultCount": metrics.get("resultCount", ""),
                "processedObjects": metrics.get("processedObjects", ""),
                "memoryStepIndex": step_index,
                "verified": verified,
                "inversions": inversions,
                "distinctIds": distinct_ids,
                "rows": rows,
                "error": err,
            })
            fh.flush()
            recorded += 1
            if recorded % 10 == 0:
                print(f"[driver] t={time.time() - started:7.1f}s recorded={recorded} "
                      f"last={metrics.get('clientElapsed', 0):.2f}s mem={current_mb:.2f}MB", flush=True)

            if args.think_time:
                time.sleep(args.think_time)

    manifest["endedEpoch"] = time.time()
    manifest["wallSeconds"] = manifest["endedEpoch"] - manifest["startedEpoch"]
    manifest["queriesRecorded"] = recorded
    manifest["finalSortMemoryMB"] = current_mb
    manifest["memorySteps"] = step_index
    with open(manifest_path, "w") as fh:
        json.dump(manifest, fh, indent=2)

    print(f"[driver] done: {recorded} queries -> {csv_path}", flush=True)


if __name__ == "__main__":
    main()
