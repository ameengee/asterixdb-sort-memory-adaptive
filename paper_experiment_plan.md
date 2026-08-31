# Memory-Adaptive Sort — Paper Experiment Plan

> **Status:** plan. Companion to `adaptive_sort_algorithm_plan.md` (the design),
> `sort_operator_deep_dive.md` (how sort works today), `sort_memory_curve_theories.md`
> (unconfirmed U-curve hypotheses), and `demo_memory_adaptive_commands.md` (the demo runbook).
>
> **Deadline:** ~2 weeks from 2026-08-30.
> **Execution:** AWS, not local. Local is for smoke tests only.

---

## 0. The claim we are defending

> We built a memory-adaptive external sort and applied it to AsterixDB. It **exploits memory
> granted at runtime**, **gives memory back cheaply when reclaimed**, and **does not degrade
> the system when no adaptation happens**.

Three conditions, in priority order:

1. **No harm.** With the broker inert, performance matches stock AsterixDB.
2. **Grows well.** When memory is granted mid-flight, we exploit it; stock cannot until the
   next query starts.
3. **Shrinks cheaply.** When memory is reclaimed, we return it fast and the damage is bounded.

Explicitly **not** claimed: that we fixed or explained the U-curve (see §6 of the design doc).
The simulated broker is fine for this paper — the contribution is the *mechanism*, not a policy.

---

## 1. The time-scale constraint (read this before designing anything)

Stock AsterixDB **does** benefit from more sort memory: `compiler.sortmemory` is read per query
at compile time, so any query that *starts* after a change gets the new budget. What stock
cannot do is help a query that is **already running**.

**Therefore the entire measurable difference lives in queries that span a memory change.**

The governing parameter is a ratio, not an absolute:

```
events_per_query  =  query_duration / memory_change_period
```

- 1.3s queries (current 300k demo) with 10-minute changes => ~0.2% of queries span an event
  => a null result for the wrong reason.
- Target **1-4 events per query**.

Make `events_per_query` the **independent variable**: sweep it from 0 to ~5 and plot benefit
against it. The curve's shape (benefit -> 0 for short queries, growing for long ones) IS the
central result, and it directly answers "what about really long-running queries?"

**Statistical note:** cloud time is cheap, but samples still matter. Many medium queries beat a
few very long ones — prefer shortening the memory-change period over lengthening queries.

---

## 2. Harness prerequisites (gates every experiment)

### 2.1 Broker policies
Currently only `RandomMemoryBroker(VICTIM_PROBABILITY=0.3, seed=0)` exists.
Needed, selectable at runtime:

- **`none`** — never grants, never reclaims. *Required for E1.* Must return 0 from both
  `getReclaimDemand()` and `requestMore()`. Assert in logs that the budget never moves.
- **`scripted`** — replays a deterministic trace of `(frameNumber | wallClock, action)` events.
  Required for reproducibility and for constructing specific scenarios.
- **`random`** — existing, fixed seed, many trials.
- **`distribution`** — normal / t-distributed grant+reclaim amounts (per Ameen's preference),
  seeded.

### 2.2 Mechanism toggles (so one build produces every configuration)
- `-Dhyracks.sort.mergeFanIn=N` — **DONE**. 2 = eager binary cascade; huge = no cascade.
- `bucketTargetBytes` — **TODO**, same system-property pattern. Huge value = Stage 1 disabled
  (one big sort at flush), which is the in-build stock-sorter baseline.
- Partial-spill on/off — **TODO**. Off => victim paths fall back to `flushFramesToRun()`,
  i.e. Stage 2 disabled.

### 2.3 Workload driver
Script that: issues a query stream, steps `compiler.sortmemory` on a schedule (for the stock
arm) or drives the broker (for the adaptive arm), and writes per-query CSV.

### 2.4 Correctness oracle on every run
Non-negotiable. Verify **sorted** AND **distinct-id count == row count** (catches tuple loss or
duplication through partial spills). Already validated at 300k rows across all fan-in values.

---

## 3. Experiments

### E1 — No harm (highest priority)
**Setup.** Stock `master` vs. adaptive with broker = `none`. Identical `compiler.sortmemory`,
query stream, dataset, JVM, GC, instance.

**Success.** Median and p95 within noise (target +/-3%).

**Notes.**
- Memory accounting is **identical** for merge sort: master's `FrameSorterMergeSort` already
  added +1x pointer memory on top of the base 1x; the branch moved that 2x into
  `AbstractFrameSorter` and disabled the override. So any delta is real CPU, not accounting.
- Expect a small CPU cost (bucket sorts + cascade copy-back) possibly offset by cache locality.
- **Report the number even if it is a small loss.** A few percent bought in exchange for
  elasticity is defensible; concealing it is not.
- Sanity check: adaptive with `bucketTargetBytes=huge` + broker `none` should approximate
  master closely. If it does not, something else differs.

### E2 — Grow benefit
**Setup.** Same query stream; memory rises 10% per interval. Stock arm: raise
`compiler.sortmemory` between queries. Adaptive arm: broker grants mid-flight.

**Measure.** Throughput and per-query latency vs. `events_per_query` (§1).
**Headline figure.** *Time-to-exploit*: wall time between a grant and the extra memory actually
being used. The `grant-grow` log lines plus `framesLoaded` on the following
`adaptive-sort-run:` line show this directly — consecutive `grant-grow` with no
`adaptive-sort-run:` between them proves the live run grew without spilling.

### E3 — Shrink cost
**Framing.** Stock has **no eviction mechanism**, so this is not head-to-head. It is a
cost/benefit curve: what the evicted query loses vs. how fast memory returns to the system.

**Setup.** Sweep eviction aggressiveness (frequency x amount).
**Measure.** Victim-query slowdown, reclaim latency, bytes spilled, runs generated.
**Expect** two distinct regimes, already visible in the demo trace:
- `victim-periodic-shrink` — tighten budget, **no spill** (cheap)
- `victim-periodic-spill` — must spill (expensive)
Showing these as separate cost tiers is a result in itself.

### E4 — Downside hunt: run-count explosion
Deliberately look for the failure mode. **Hypothesis:** every partial spill emits a run; evict
often enough and run count exceeds the single-pass merge width
(`maxMergeWidth = framesLimit - 1`, ~1023), forcing a multi-pass merge that MASORT calls
"virtually never needed."

**Setup.** Fixed workload, sweep eviction frequency. Plot runs-generated and total time; find
where it breaks.

A paper that bounds its own failure mode reads as more rigorous than one claiming a free lunch,
and it writes its own future-work paragraph.

### Supporting (only if time permits)
- **Bucket size sweep** (design doc open Q2). Fits-in-memory so no I/O confound. Note that
  `ptrSize` ranges 3->7 ints by key type while `currentBucketBytes` counts only frame bytes, so
  a "256KB bucket" has a cache footprint that varies >2x with the sort key's type.
- **Merge fan-in sweep** (open Q3). Note `mergeTopRuns` writes to `tScratch` then copies back —
  2 pointer moves per tuple per level — so eager may cost more memory traffic than the design
  doc's "eager and lazy do the same work" claim assumes. Measuring this is worthwhile either way.

---

## 4. Metrics to capture

**Per query** (free in the REST JSON response):
`elapsedTime`, `executionTime`, `compileTime`, `queueWaitTime`, `resultCount`, `processedObjects`

**Per sort run** (from `adaptive-sort-run:` log lines):
`framesLoaded`, `bytesUsed`, `budgetBytes`, `fillPct`, `tuples`, `mergeFanIn`, `bucketTargetBytes`

**Per broker decision** (from `adaptive-sort:` lines): one of
`grant-grow`, `victim-full`, `victim-periodic-shrink`, `victim-periodic-spill`, `denied`

**System:** wall clock, **total CPU** (not just wall), GC log (`-Xlog:gc`), bytes written to the
run-file directory, device-level I/O.

**Derived:** throughput (queries/hr), p50/p95/p99 latency, runs per query, merge passes.

---

## 5. Methodology

- **Repetitions over duration.** 5 x 30min beats 1 x 3h — you need variance, not a long line.
- Report **median + spread**, never a single number. Discard JVM warmup.
- **Page cache is a variable.** Spilled runs are re-read almost immediately; whether they sit in
  page cache changes everything. Measure cold and warm deliberately, do not let it vary by luck.
- **GC config is first-class**, not a footnote — all sort memory is heap
  (`FrameManager.allocateFrame` uses `ByteBuffer.allocate`), and GC is suspect #1 in the U-curve
  notes. Report the collector; ideally test two.
- **Restrict to `Algorithm.MERGE_SORT`** and say so. Stage 1 routes everything through
  `sortBucketSlice` (a stable merge), so `QUICKSORT` silently behaves as merge sort on this
  branch. Do not report quicksort numbers.
- **Real data.** TPC-H `lineitem` at a few scale factors, sorted on columns of different types
  (INTEGER=1 int, BIGINT/DOUBLE=2, UUID=4, STRING=1 but indecisive). The 300k synthetic rows are
  a smoke test, not a paper workload.
- **Verify the code under test is actually loaded.** See `sort-testing-traps` — `mvn -pl` on a
  test module resolves `hyracks-dataflow-std` from `~/.m2`, and `JAVA_OPTS` does not reach the
  NC JVMs. The `adaptive-sort-run:` line echoes `mergeFanIn` and `bucketTargetBytes` for exactly
  this reason: every run self-reports its config.

---

## 6. Cloud setup

**Instance:** C-series (compute-optimized, **non-burstable**). T-series CPU credits throttle
after sustained load and would silently corrupt multi-hour timings.

**Disk:** prefer a `d`-suffixed instance with **local NVMe** (`c6gd`, `c5d`) so run-file writes
hit a consistent device. EBS gp3 has burst dynamics that corrupt I/O measurements the same way
CPU credits corrupt timings. If EBS is unavoidable, use provisioned IOPS and report the setting.

**Reproducibility (a real advantage of doing this on AWS):** naming an exact instance type is
more reproducible than describing local hardware. Anyone can rent the identical machine.
Report: instance type, region, AMI/OS version, JDK version, GC, EBS/NVMe config.

**Cost:** ~$3-5 for 24h. Duration is not a constraint; consistency is.

**Set NC JVM flags via `jvm.args` under `[nc]` in `<CLUSTER>/conf/cc.conf`**, e.g.
`jvm.args=-Xmx4g -Dhyracks.sort.mergeFanIn=8`. `JAVA_OPTS` is silently ignored.

---

## 7. Schedule (~2 weeks)

| Days  | Work |
| :---- | :--- |
| 1-2   | Harness: broker policies (`none`/`scripted`/`distribution`), `bucketTargetBytes` + partial-spill toggles, workload driver, CSV metrics |
| 1     | **Start the TPC-H load in parallel** — it is the long pole |
| 3-4   | Pilot at 5-minute scale locally; shake out bugs before burning cloud hours |
| 5-8   | Real runs on AWS (E1, E2, E3, E4) |
| 9-11  | Analysis and plots |
| 12-14 | Writing, with slack |

---

## 8. Caveats to disclose in the paper

- The broker is **simulated**; policy is future work. The contribution is the mechanism and the
  3-tier cost signal (`MemoryStatus`: easy / medium / hard), not a scheduling policy.
- Single node. AsterixDB is shared-nothing; cross-partition behavior is not evaluated.
- The U-curve is **not** explained or fixed here. Report measurements without attribution.
- Merge sort only (see §5).
- `adaptiveMinFrames = min(maxSortFrames, 16)` and `adaptiveMaxFrames = maxSortFrames * 4` —
  the pool is built at the ceiling so "grow" has frames to hand out. Note this in the setup;
  it means the adaptive arm reserves a larger pool up front.

---

## 9. Preliminary result log

### E1 pilot -- 2026-08-30, LOCAL MacBook, 300k rows, 8MB sort memory
**Setup:** 3 alternating rounds x 180s per arm; stock = master jar, adaptive = branch jar with
broker `none`. ~825 queries per arm.

| metric | stock | adaptive | delta |
| :-- | --: | --: | --: |
| p50 | 622.6ms | 628.3ms | +0.92% |
| mean | 628.4ms | 637.0ms | +1.37% (z=5.0, significant) |
| p95 | 664.1ms | 680.8ms | +2.52% |
| p99 | 699.5ms | 787.3ms | +12.55% |

**But the pooled numbers are dominated by round 1.** Per-round p99 delta: rr1 **+157ms**,
rr2 -37ms, rr3 -12ms. Excluding round 1: **mean delta +0.02%**, p99 delta +3.1%.

**Reading:** no meaningful harm once warm. The adaptive arm has more code to JIT
(`buildFramePointers`, `sortBucketSlice`, `mergeTopRuns`), so it needs a longer warmup than
stock; `--warmup 3` was far too short. Default raised to 40 queries in `run_e1.sh`.
This is exactly what the ALTERNATING design was for -- a blocked run would have reported a
1.4% regression as a real effect.

**Not yet answered: the sawtooth.** cpu_cv came out 0.556 (adaptive) vs 0.542 (stock) -- i.e.
indistinguishable -- but that measurement cannot answer the question: 100ms sampling across
0.6s queries gives ~6 samples per query, which blurs intra-query shape entirely. Resolving it
needs `profile_query.py` against a dataset large enough for multi-second queries.

**Caveats:** local laptop (thermal drift, no CPU pinning), 300k rows, short queries, macOS
system-wide disk counters. Treat as pipeline validation, not a paper result.

### Sort-operator phase behavior -- 2026-08-30, EC2 c6gd.2xlarge, 10M rows, 512MB sort memory
Measured with in-sorter instrumentation (`-Dhyracks.sort.phaseLog=true`), same binary both sides;
`bucketTargetBytes` huge reproduces stock's load-then-sort path.

| | load-then-sort (bucketing off) | interleaved (bucketing on) |
| :-- | --: | --: |
| sortEvents | 1 | 325 |
| spreadPct  | 0 | 85-86 |
| sortNs     | 16.12s | 7.76s |
| mergeNs    | 0 | 2.97s |

Consistent across all runs of repeated queries. **This is the interleaving result**: stock performs a
single monolithic sort at flush; the bucketed sorter performs 325 sorts spread across 86% of the
run's lifetime. In-sorter CPU also drops 33% (16.12s -> 10.73s sort+merge).

**Why a CPU sampler cannot show this** (and an earlier analysis of ours wrongly concluded "no
effect"): during the load phase the operator thread is running `System.arraycopy`, so it reads as
~100% busy in BOTH designs. Process-level CPU shows which thread is active, not which kind of work.
Marginal statistics (mean/max/cv) are worse still -- they discard time ordering entirely, and a
sawtooth is purely a temporal structure.

**Open paradox worth chasing:** bucketing reduces in-sorter CPU by 33% yet the end-to-end query is
~8% SLOWER at 512MB (20.11s vs 18.59s stock). The extra time is therefore NOT in the sorter's own
sort/merge work. Locate it before optimizing -- two hypothesis-driven optimizations were attempted
and both were wrong:
  - cascade copy-back: refuted (the fan-in sweep that appeared to support it was invalid, see below)
  - per-bucket copy-back: an in-place insertion-sort variant measured 3.6% SLOWER (20.84s vs 20.11s)
    because insertion sort shifts ptrSize-wide slots, moving ~5-7x more data than the merge passes
    it replaced.

### Invalidated experiment -- fan-in sweep, 2026-08-30
A 512MB fan-in sweep (2/8/32/no-cascade) was VOID: `MERGE_FAN_IN` was a hardcoded constant at the
time, so `-Dhyracks.sort.mergeFanIn` was ignored and every arm ran fan-in 2. All 3876 log lines
echoed `mergeFanIn=2`. Caught only because the run log echoes its own configuration -- keep that
echo, and check it before trusting any sweep.

### Corrected accounting -- 2026-08-31 (supersedes the "33% less CPU" claim above)
The cascade merges called from `sealBucket` were NOT instrumented. Counting them changes the
conclusion. Per partition, 5.0M tuples, 512MB sort memory:

| stage | bucketed | load-then-sort |
| :-- | --: | --: |
| bucket sorts | 6.20s | - |
| **cascade merges** | **6.28s** | 0 |
| final merge | 2.69s | 0 |
| single monolithic sort | - | 13.99s |
| **total sort work** | **15.16s** | **13.99s** |
| blocked on upstream (gapNs) | 3.97s | 4.02s |

**Total sorting work is roughly EQUAL.** Bucketing does not reduce comparison work; it restructures
one big sort into many small sorts plus a merge cascade plus a final merge. The earlier "33% less"
figure counted only `sortBucketSlice`.

**Merge work is conserved.** Raising the cascade fan-in moves time out of the cascade and into the
final merge almost one-for-one, and query time does not improve:

| fanIn | cascade | final merge | merge total | query |
| :-- | --: | --: | --: | --: |
| 2 | 6.24s | 2.91s | 9.15s | 22.02s |
| 8 | 4.52s | 6.03s | 10.55s | 22.62s |
| 32 | 3.42s | 5.41s | 8.83s | 21.35s |
| none | 0.00s | 8.96s | 8.96s | 22.17s |

**How much slower:** ~8-9% at 512MB; indistinguishable at 8MB. The gap grows with sort memory,
i.e. with bucket count. It is NOT upstream starvation (gapNs is identical) and NOT the external
merge (time outside the sorter is small). It is the overhead of the extra structure.

**What survives for the paper:** the interleaving result (325 sort events at 84% spread vs 1 at 0%)
and lower time-averaged memory occupancy (117MB vs 171MB, **32% lower**) -- see
`experiments/figures/`. Drop any "less CPU" claim.

### Two failed optimizations (do not retry without measuring)
1. **Cascade copy-back**: hypothesized as the cost; refuted -- fan-in has no effect on query time.
2. **Per-bucket copy-back**: replacing the copy-back with in-place insertion runs + even-parity
   merging measured 3.6% SLOWER (20.84s vs 20.11s). Insertion sort shifts `ptrSize`-wide slots,
   moving ~5-7x more data than the merge passes it replaced.

### Fan-in is a free knob (figure: `experiments/figures/fig3_fanin`)
Query time is flat across cascade fan-in 2 / 4 / 8 / 32 / none. Spread between settings is 0.65s
while spread *within* a single setting reaches 1.81s -- the settings are indistinguishable. Merge
work is conserved: raising fan-in moves seconds out of the cascade and into the final merge almost
one-for-one (6.24 -> 0.00 cascade, 2.91 -> 8.96 final; total stays ~9s).

**Why this matters:** because fan-in costs nothing in throughput, it can be chosen entirely for
adaptivity behaviour rather than for speed.

- **Small fan-in (eager, 2)** merges aggressively during arrival, so only ~log2(buckets) runs are
  outstanding and a large already-sorted run is nearly always available to spill. That should make
  victim response cheap -- the shrink primitive has little left to merge before it can give memory
  back.
- **Large fan-in (lazy)** leaves more unmerged runs outstanding, so a victim spill must do more
  merging first, but it performs fewer cascade events during normal operation.

**Status: the adaptivity half is REASONED, NOT MEASURED.** We have measured only that fan-in does
not affect query time. Victim-response latency as a function of fan-in is experiment E3 and has not
been run. Do not state the adaptivity tradeoff as a result until it is.
