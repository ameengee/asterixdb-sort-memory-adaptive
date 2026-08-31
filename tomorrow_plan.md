# Next Session Plan — Memory-Adaptive Sort

Written 2026-08-31 at end of session. Instance terminated; all data salvaged to
`experiments/results/`. Read with `paper_experiment_plan.md` (the standing plan) and
`experiments/README.md` (how to run everything).

---

## 0. Where we got to

**The k-way tournament merge is the headline result.** Replacing balanced-pairwise merging with a
one-pass k-way tournament (`-Dhyracks.sort.kwayMerge=true`) turned an 8.95% regression into a ~6-7%
WIN over stock. Measured at 512MB sort memory, 6 reps per config, jars verified per config:

| config | median | vs stock |
| :-- | --: | --: |
| stock | 19.89s | - |
| nobucket (bucketing off) | 19.95s | +0.30% |
| fi2 (bucketed, pairwise) | 21.67s | **+8.95%** |
| fi32 pairwise | 20.91s | +5.10% |
| **fi32 k-way** | **18.52s** | **-6.89%** |
| huge fan-in pairwise | 21.87s | +9.93% |
| **huge fan-in k-way** | **18.70s** | **-5.99%** |

Head-to-head, same config, merge strategy only: **-14.5%** at huge fan-in, **-11.4%** at fan-in 32.

**Why it works.** Balanced pairwise merging of k runs rewrites every tuple on every pass:
N*log2(k) moves. A k-way tournament does the same N*log2(k) comparisons but writes each tuple
ONCE: N moves. At 325 runs that is ~8x less pointer traffic, and a pointer slot is ptrSize ints
(20-28 bytes), so movement is what actually hurts. Confirmed by counters: at huge fan-in + k-way,
16.00 moves/tuple and 20.86 compares/tuple with **residual 0.00s** (full stage coverage).

**Correctness verified** for k-way at fan-in 8 and huge fan-in, at 1/8/64MB: sorted, zero
inversions, no tuple loss, stability identical to stock.

---

## 1. IMPORTANT CAVEAT before claiming "strong benefits"

**Every headline number above was measured at 512MB sort memory. AsterixDB's default is 32MB.**
(`CompilerProperties.COMPILER_SORTMEMORY` = 32MB per sort operator per partition; min 512KB.)
512MB is **16x the default**.

The k-way advantage should SHRINK as sort memory shrinks, because it scales with the number of runs
being merged:

| sort memory | approx buckets | pairwise merge passes | k-way passes | expected k-way win |
| :-- | --: | --: | --: | :-- |
| 3.2MB (0.1x default) | ~2 | ~1 | 1 | ~none |
| 32MB (default) | ~20 | ~4.3 | 1 | modest |
| 320MB (10x default) | ~200 | ~7.6 | 1 | large |
| 512MB (what we measured) | ~325 | ~8.3 | 1 | large |

So the honest current claim is: **no harm at default, growing benefit as sort memory grows** --
and the "no harm at default" half is NOT yet measured with k-way. Ameen's three-level plan
(0.1x / 1x / 10x default) is exactly the right fix and should be the primary axis of every
experiment below.

**Second caveat: tuple width.** All measurements use the `count(*)` wrapper, which lets the
optimizer project the payload away -- tuples through the sort are ~17 bytes (1927 per 32KB frame).
A record-returning ORDER BY carries the payload. Both arms are affected equally so the comparison
is fair, but the absolute regime differs. Add a payload-preserving shape:
`SELECT VALUE count(x.payload) FROM (SELECT VALUE r FROM ds AS r ORDER BY r.k) AS x;`

---

## 2. What "Pass A" and "Pass B" mean (they are just the two halves of a matrix run)

`experiments/run_matrix.sh` runs each configuration twice, for a reason:

- **Pass A — timings, instrumentation OFF** (`--phase-log false`). Clean query times that the
  counters cannot perturb. This is what the comparison tables come from.
- **Pass B — instrumented, ONE run per config** (`--phase-log true`). Collects:
  - per-stage times: `sortNs` (bucket sorts), `cascadeNs`, `mergeNs` (final merge), `flushNs`
  - **operation counters**: `moves` (tuple-slot copies) and `compares`
  - `residualNs` = runSpan - (gap + insert + sortCall). Non-zero means a stage is uninstrumented.
  - the 100ms `adaptive-sort-series` time series that figures 1 and 2 are built from

They are separated so instrumentation overhead never contaminates the headline timings. Pass B is
also where figure 4's raw data comes from.

---

## 3. Tomorrow's experiment list

Run everything at **three memory levels: 3.2MB (0.1x), 32MB (default), 320MB (10x)** -- plus 512MB
if time permits, to connect with today's numbers. All arms use k-way now, so **every earlier result
must be re-established**; the old numbers were pairwise.

### E1. No harm (re-run, with graphs)
Stock vs adaptive-with-inert-broker, k-way on. **Use both baselines:**
  - `stock` (master jar) -- the real comparison
  - `nobucket` (same jar, `--bucket-bytes 999999999999`) -- isolates the sorter change from any
    build difference, since it is the SAME binary
Alternating blocks. Deliverable: a memory-level x arm figure with CIs.

### E2. Merge fan-in sweep (re-run, many more trials)
Fan-in 2 / 4 / 8 / 32 / none, k-way on, at each memory level. Ameen is right that 3 reps is not
enough. **Power math:** query sd is ~0.2s on a ~20s query (1%). SE = sd/sqrt(n).
  - n=3  -> SE 0.58% -> can only resolve ~1.7% differences
  - n=10 -> SE 0.32% -> ~0.9%
  - n=50 -> SE 0.14% -> ~0.4%
n=50 costs 50 x 20s = ~17 min per config; 5 configs x 3 memory levels = ~4.2 hours. Feasible on a
~$0.30/hr instance. **Suggest n=30 as the sweet spot** (SE 0.18%, ~2.5h) unless we specifically
need to resolve <0.5%.
Note this is an EQUIVALENCE claim ("fan-in does not matter"), so state a margin up front -- e.g.
"all settings within +/-1% of each other" -- rather than just failing to find a difference.

### E3. Figures 1 and 2 (re-run with k-way)
The I/O-vs-CPU figure and the memory-occupancy figure were both generated from pairwise traces.
Re-collect `adaptive-sort-series` with k-way at each memory level and regenerate via
`make_figures.py`. Expect the interleaving story to hold (that is about WHEN sorting happens, not
HOW merging is done) but the memory-occupancy numbers to shift.

### E4. Grow benefit (never run)
Broker grants memory mid-flight; stock can only benefit at query boundaries. Governing variable is
`query_duration / memory_change_period` -- target 1-4 events per query and SWEEP it.
`./deploy.sh --broker periodic --action grant` plus `run_workload.py --grow-pct/--grow-every`.

### E5. Release speed (never run)
`--broker periodic --action reclaim`, sweeping the period. Measure victim-query slowdown vs how
fast memory is returned. Also the downside hunt: does run count explode under frequent eviction and
break the single-pass external merge (`maxMergeWidth = framesLimit - 1`)?
**This is also where the fan-in adaptivity claim gets tested** -- small fan-in should give cheaper
victim response because a large sorted run is always spill-ready. Currently REASONED, NOT MEASURED.

### E6. The U-curve (Dr. Jahangiri's observation)
Stock AsterixDB reportedly gets faster with more sort memory up to a point, then SLOWER. Never one
of our research questions, but now worth testing because our change plausibly interacts with it.

Design: sweep sort memory wide (1MB, 4, 16, 32, 64, 128, 256, 512MB, 1GB, 2GB) for stock and for
ours, at fixed data size. Two variants:
  - **spilling** (data > memory) -- the normal case
  - **fits-in-memory** (memory > data) -- isolates pure CPU/cache effects from I/O, which is the
    cleanest way to see whether the upturn is algorithmic or I/O-related
Hypotheses in `sort_memory_curve_theories.md`: GC pressure, cache/TLB collapse, page-cache
eviction, over-wide merge fan-in.

**Why our change might genuinely help the right-hand side of the U:** two of those hypothesised
causes are cache locality (#2) and over-wide merges (#4). Bucketing makes the sort phase
cache-resident, and k-way cuts merge pointer traffic ~8x -- and our advantage GROWS with memory,
which is exactly where the U turns up. If stock shows an upturn and ours does not, that is a strong
result. **Do not assume it.** Report whatever happens, including "no U-curve reproduced".

Watch out: `adaptiveMaxFrames = maxSortFrames * 4` and the pool is built at that ceiling, so very
large sort memory needs a correspondingly large `--heap` or it will OOM.

---

## 4. Things not yet on the list, worth considering

1. **Key-type sweep.** `ptrSize` ranges 3->7 ints by sort-key type, which changes pointer traffic
   and therefore how much k-way helps. `load_data.py` already creates `k_int` (INTEGER, 1 int),
   `k_big` (BIGINT, 2), `k_dbl` (DOUBLE, 2), `k_str` (STRING, 1 but INDECISIVE). One sweep would
   show whether the win is bigger for wide keys, as theory predicts.
2. **A clean stability test.** Our stability check shows 7 order violations for stock AND for every
   adaptive variant -- a partition-merge artifact, not a sorter bug. A single-partition cluster
   would give an unambiguous stability result, which a reviewer will want.
3. **Wide-tuple shape** (see caveat in section 1).
4. **Report the noise floor.** 42 samples of an identical config gave sd 0.10-0.22s and 0.67% total
   spread. Quote that as the measurement floor so "no difference" claims have a scale.

---

## 5. Future work section (Ameen's idea, worth writing down)

- **Offset-Value Coding (OVC)** to speed the k-way merge: cache the offset at which two keys first
  differ so later comparisons skip the common prefix. Pairs naturally with the tournament tree and
  with the existing normalized-key fast path.
- **Powersort / Timsort-family run detection** inside the bucket sorts, to exploit pre-sorted input.
- **AlphaSort-style cache-sized sorting** refinements to the bucket boundary (currently
  `currentBucketBytes` counts only frame bytes and ignores the pointer slice, which is
  key-type dependent -- see `paper_experiment_plan.md`).

---

## 6. Rebuild recipe (~25 min to a working cluster)

```bash
# 1. launch c6gd.2xlarge, us-east-1, Ubuntu 24.04 arm64, via experiments/aws_ue1.sh
# 2. IMPORTANT: mount the instance store by "largest disk with NO mounted partitions".
#    NVMe device naming is NOT stable -- nvme0n1 was the instance store on one launch and the EBS
#    root on another. Hardcoding a device name silently puts everything on burstable EBS.
# 3. clone branch sort-memory-adaptive to /mnt/nvme/asterixdb
# 4. mvn -T 1C package -DskipTests -pl asterixdb/asterix-server -am -Drat.skip=true ...
#      (asterix-doc races under -T and is not needed)
# 5. stock baseline jar: git worktree add --detach /mnt/nvme/master-base 5a9f1c96e
#      then mvn -pl hyracks/hyracks-dataflow-std -am package   <-- -am IS REQUIRED, siblings are
#      not installed to ~/.m2, and plain -o will fail to resolve them
# 6. ./load_data.py --mode synthetic --rows 10000000 --batch 1000000 --drop
# 7. ./deploy.sh --jar <jar> --broker none --heap 4g --kway true [--phase-log true]
```

## 7. Harness rules learned the hard way (do not skip these)

Every one of these cost us real time this session:

1. **`deploy.sh` verifies the deployed jar's md5** against the requested one. An earlier matrix run
   measured the STOCK jar under seven different labels because a failed deploy silently left the
   previous jar in place. Never trust a config label; check the md5 line.
2. **A failed deploy must be fatal.** `run_matrix.sh` now aborts and requires the `jar verified`
   line. Redirecting deploy output to /dev/null is how #1 happened.
3. **Verify the log format matches its argument count.** A silently-missed edit left `moves` and
   `compares` incrementing but never printed. Assert placeholders == args.
4. **Patch scripts must assert every anchor matched**, not just "the file changed" -- a coarse
   `s != orig` check passed while one replacement silently missed.
5. **`grep` under `set -euo pipefail` kills the script** when it finds nothing. An earlier run died
   silently after part A and never reached parts B and C.
6. **Never use `pgrep -f <script>` for liveness** -- it matches the monitor's own command line. Use
   an explicit status file plus a heartbeat with a stall detector.
7. **`JAVA_OPTS` never reaches the NC JVMs.** Use `jvm.args` under `[nc]` in `cc.conf`.
8. **Large results OOM the client and can take the cluster down.** Use the `count(*)` wrapper; a
   10M-row record-returning query materialises ~2.2GB.
9. **`mvn -pl <module>` alone resolves siblings from `~/.m2`**, which may be stale or missing. Use
   `-am`, and do not use `-o` unless the deps are known to be present.
