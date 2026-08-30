# Memory-Adaptive Sort — Experiment Harness

Everything needed to run the experiments in `../paper_experiment_plan.md`. One deployed jar
covers every arm; the configuration is JVM system properties, so no rebuild between runs.

## Pieces

| File | Purpose |
| :--- | :--- |
| `deploy.sh` | Build (optional), deploy the jar, rewrite `cc.conf`, restart the cluster, **and verify the config actually reached the NC** |
| `run_workload.py` | Issue a query stream, optionally step `compiler.sortmemory`, write per-query CSV + manifest |
| `scrape_sort_logs.py` | Turn NC log lines into `decisions` / `runs` / `broker` CSVs |
| `broker_script_example.csv` | Example trace for the scripted broker |
| `load_data.py` | Load synthetic (multi-key-type) or TPC-H lineitem datasets |
| `sample_pressure.py` | Sample NC CPU / disk / RSS over time |
| `profile_query.py` | Profile ONE query at high frequency -- the sawtooth figure |
| `run_e1.sh` | Run the E1 no-harm comparison with alternating arms |
| `analyze_e1.py` | Summarize E1 latency + pressure |

## Why `deploy.sh` exists

Two traps make it very easy to measure the wrong thing, both silently:

1. **`JAVA_OPTS` never reaches the NC JVMs** — the NCService spawns them. Settings must go in
   `jvm.args` under `[nc]` in `cc.conf`. `deploy.sh` writes that line for you.
2. **A stale jar in `~/.m2` or the cluster `repo/`** will happily run old code.

So after restarting, `deploy.sh` issues one throwaway sort and greps **only the newly added log
lines** for `adaptive-sort-broker: policy=...`. If the policy does not match what you asked for,
it **exits non-zero**. Reading the whole log would match a previous arm's line and give a false
confirmation — that actually happened during development.

Note the confirmation query has **no `LIMIT`**: a `LIMIT` routes to the top-K sorter
(`HeapSortRunGenerator`), which never constructs the adaptive run generator.

## Knobs

All are JVM system properties; `deploy.sh` flags map onto them 1:1.

```
hyracks.sort.broker = random | none | periodic | scripted | distribution   (default random)
  none          inert -- never grants or reclaims. The E1 "no harm" control arm.
  random        existing shell: victim/grant/deny by probability, amount = half the budget
                  .victimProbability (0.3)  .seed (0)
  periodic      deterministic: acts every Nth broker interaction. The sweepable arm for E3/E4.
                  .period (10)  .action (reclaim|grant)  .fraction (0.5)
  scripted      replays an exact trace; use for reproducing a named scenario
                  .script (path)
  distribution  amounts drawn from a normal or t distribution (t = heavier tails)
                  .victimProbability (0.3) .distribution (normal|t) .mean (0.5)
                  .stddev (0.15) .df (5) .seed (0)

hyracks.sort.bucketTargetBytes  (262144)  cache-sized bucket target.
                                          A huge value => no bucket seals during accumulation,
                                          so sort() does ONE big sort == Stage 1 DISABLED.
hyracks.sort.mergeFanIn         (2)       cascade fan-in. 2 = eager binary;
                                          huge = no cascade (one merge at the end).
hyracks.sort.partialSpill       (true)    false => victims do a full flush == Stage 2 DISABLED.
hyracks.sort.victimCheckInterval(10)      poll the broker every N frames.
```

## Experiment arms

```bash
# E1 -- no harm. Budget must never move; expect decision mix to be 100% "denied".
./deploy.sh --broker none
./run_workload.py --label e1-adaptive --duration 1800 --sort-memory 8MB
# (stock arm: same driver command against a master-branch build)

# E2 -- grow. Broker grants mid-flight; driver also steps stock memory for the comparison arm.
./deploy.sh --broker periodic --period 12 --action grant --fraction 0.5
./run_workload.py --label e2-adaptive --duration 1800 --sort-memory 4MB \
    --grow-pct 10 --grow-every 30

# E3 -- shrink cost. Sweep the period; lower = more frequent eviction.
for P in 4 8 16 32 64; do
  ./deploy.sh --no-build --broker periodic --period $P --action reclaim --fraction 0.5
  ./run_workload.py --label e3-p$P --duration 600 --sort-memory 8MB
done

# E4 -- downside hunt: does run count explode under frequent eviction?
#      Same sweep as E3; plot runs-per-query from the .runs.csv against period.

# Mechanism ablations
./deploy.sh --broker none --bucket-bytes 999999999999   # Stage 1 off (stock sorter in-build)
./deploy.sh --broker periodic --partial-spill false     # Stage 2 off (full flush on victim)
for N in 2 4 8 16 1000000; do ./deploy.sh --no-build --merge-fan-in $N; done
```

## Isolating one run's logs

NC logs accumulate. Pass the same `--since-line-file` before and after a run; the scraper stores
each log's line count so the next scrape starts where the last ended.

```bash
CLUSTER=.../opt/local
./scrape_sort_logs.py --logs "$CLUSTER/logs" --label _reset --outdir /tmp/x --since-line-file run.since
./run_workload.py --label myrun --duration 600
./scrape_sort_logs.py --logs "$CLUSTER/logs" --label myrun --since-line-file run.since
```

## Correctness

`run_workload.py --verify` downloads full results and checks **sortedness** and **distinct id
count == row count** (the second catches tuple loss or duplication through partial spills).

Leave `--verify` OFF for timing runs — the transfer and check dominate the measurement. Use it
for a short validation pass after every configuration change.

## Smoke-test results (2026-08-30, local MacBook, 300k rows, 8MB)

| Arm | Decision mix | Correctness |
| :--- | :--- | :--- |
| `none` | denied=396 only; budget constant at 255 frames | 65/65 sorted, 0 inversions, 300k ids |
| `periodic` reclaim p=8 | victim-full, -shrink, -spill all fire | 35/35 sorted, 0 inversions |
| `periodic` grant p=12 | grant-grow=98 | grow observed alongside driver steps |
| `distribution` t(df=4) | all five paths; 32 distinct budget levels, 16..86 frames | 28/28 sorted |
| Stage 1 off | bucketTargetBytes echoed as 999999999999 | 29/29 sorted |
| Stage 2 off | victims full-flush | 29/29 sorted |

Local numbers are for wiring validation only — real measurements belong on the AWS instance
described in `../paper_experiment_plan.md` section 6.


## Measuring resource *shape* (the sawtooth)

The claim behind Stage 1 is not only "as fast as stock" but "spreads the work differently":

* **stock** accumulates frames (disk busy, CPU idle), then sorts the whole batch at once
  (CPU spike, disk idle), then writes the run -- an interlocking sawtooth, repeated per run.
* **adaptive** sorts each cache-sized bucket as it fills, so CPU work overlaps arrival.

Two tools:

```bash
# (a) whole-run pressure, sampled alongside a workload
./sample_pressure.py --label myrun --duration 300 --interval 0.1

# (b) ONE query at high resolution -- the actual figure. Use a LOW sort memory so the sort
#     spills repeatedly and the pattern repeats visibly.
./profile_query.py --label stock-profile    --sort-memory 1MB --interval 0.02
./profile_query.py --label adaptive-profile --sort-memory 1MB --interval 0.02
```

`profile_query.py` prints an ASCII CPU-shape plot so the pattern is readable without plotting,
and writes a CSV with a `phase` column (idle / query) for real plots.

**Per-process disk counters are Linux-only.** On macOS the samplers fall back to system-wide
counters and label the mode in the CSV (`ioMode`), so a local reading is never silently mistaken
for a per-process one. Real I/O numbers should come from the Linux AWS instance.

## Comparing against stock

`deploy.sh --jar <path>` swaps a prebuilt jar without rebuilding, so A/B blocks can alternate
quickly:

```bash
git worktree add /tmp/master-base master
( cd /tmp/master-base/hyracks-fullstack && mvn -o -q -pl hyracks/hyracks-dataflow-std package -DskipTests )
cp /tmp/master-base/hyracks-fullstack/hyracks/hyracks-dataflow-std/target/*.jar /tmp/jars/master.jar
cp hyracks-fullstack/hyracks/hyracks-dataflow-std/target/*.jar /tmp/jars/adaptive.jar

ROUNDS=3 BLOCK=180 ./run_e1.sh && ./analyze_e1.py
```

`run_e1.sh` **alternates** arms (stock, adaptive, stock, adaptive, ...) rather than running all
of one then all of the other. Machine state drifts -- thermal, page cache, background load -- and
a blocked design would silently attribute that drift to the treatment.

**Do not run builds, data loads, or other heavy work while a measurement block is in flight.**
