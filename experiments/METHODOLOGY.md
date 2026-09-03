# Memory-Adaptive Sort: methodology, experiments, and findings

Written 2026-09-03. This is the durable record of *how* we run experiments, *what each one is for*,
*what we found*, and *which traps we already fell into*. If context is lost, start here.

---

## 1. What the project is

Make AsterixDB's external sort **memory-adaptive**: able to give memory back mid-query when the
system needs it, without wrecking its own performance, and without harming the default case.

Along the way we found a second, separable problem — AsterixDB frequently fails to use normalized
keys at all — which turned out to have a much larger performance effect than the adaptivity work.

**Constraint on the paper:** a teammate is building the real memory broker, and that is *his* paper.
We may not mention, cite, or depend on it. Our simulated brokers are **experimental apparatus for
exercising the sort's interface**, never presented as a contributed component.

---

## 2. The two contributions, kept separate

### A. Normalized-key auto-detection (large effect, simple mechanism)
`NormalizedKeyComputerFactoryProvider` dispatches on a single type tag. Three situations fall to
`default:` and get **no normalized key at all**:
1. **undeclared column** (open type) — static type `ANY`
2. **declared nullable column** (`k: int64?`) — static type `UNION`
3. **genuinely multi-type column** — cannot be declared at all

Case 2 matters most: the penalty comes from the **declaration, not the data**. A nullable column
containing *zero nulls* still loses the key (verified: `ptrSize=3 nkcs=none`).

`DynamicNormalizedKeyComputerFactory` (asterix-om) detects the type per tuple and builds a key with
an **ordering class** in the high bits and the value below, so mixed types sort correctly in one key
space. No bucketing required.

### B. Bucketed / memory-adaptive sort (the original goal)
Incremental cache-sized bucket sort + cascade merge, so the sort always has something already sorted
to surrender. Gives the **neighbor property** and makes **bounded memory reclamation free**.

---

## 3. How to run an experiment (the discipline that works)

1. **Interleave arms within each round.** Cross-deploy drift is ~5%; block-by-block ordering folds
   it straight into the arm difference.
2. **Verify the jar and the config, every deploy.** `deploy.sh` md5-verifies every modified module
   (`hyracks-api`, `hyracks-dataflow-std`, `asterix-om`) and confirms the broker line from the NC log.
   Deploying only one module once meant *the entire matrix measured the stock jar under seven labels*.
3. **Verify the configuration actually engaged**, from logs, not flags: `adaptive-sort-keys:
   ptrSize=... nkTotalLen=...`, `runtimeDecisive=`, `bucketTargetBytes=`.
4. **Assert dataset row counts** before measuring.
5. **Warm-up query per cell, discarded.**
6. **Reject implausible timings.** A 10M-row sort cannot finish in <0.5s; a 290M-row sort cannot in
   <1s. Such a "time" is a failed request and must abort, not be recorded.
7. **Health-check the cluster** after each deploy and on suspicious results.
8. **Never overwrite a script (or `deploy.sh`) while it is executing.** Bash reads by byte offset;
   an `scp` mid-run makes the shell resume at a meaningless position. Stage under a different name
   and swap between runs.
9. **Never run two instances against one cluster.** Kill and verify before relaunching.
10. **Detach everything** (`setsid nohup ... < /dev/null &`, PPID 1) so runs survive session loss.

### Log snapshots
To attribute log lines to one query, record **per-file offsets** and tail each file:
```
snap(){ : > .snap; for f in $CL/logs/nc-*.log; do echo "$f $(wc -l < "$f")" >> .snap; done; }
since(){ while read -r f n; do tail -n +$((n+1)) "$f"; done < .snap; }
```
`wc -l` of `cat nc-*.log` then `tail -n +N` on the re-concatenation is **wrong** — older content
leaks in. Also snapshot **after** any cluster restart, since restarts create fresh logs.

---

## 4. Experiment catalogue

| script | question it answers |
|---|---|
| `analysis/scripts/run_matrix.sh` | 13 (config x dataset) cells -> the 12 analysis graphs |
| `analysis/scripts/run_tpcds_sort.sh` | real TPC-DS, official schema, vs a non-nullable control |
| `analysis/scripts/build_tpcds_control.sh` | 23-column non-nullable control (differs ONLY in `?`) |
| `analysis/scripts/load_tpcds.sh` | DuckDB `dsdgen` -> AsterixDB, official schema |
| `analysis/scripts/run_cast_workaround.sh` | can a user cast their way out of the nullable hole? |
| `analysis/scripts/run_adaptivity.sh` | neighbor property (spreadPct) + shrink under reclamation |
| `analysis/scripts/run_realistic_reclaim.sh` | cost of a **bounded** memory demand (scripted broker) |
| `analysis/scripts/run_shrink.sh` | does `shrinkTo` actually return frames, correctly and free? |
| `analysis/scripts/run_bump.sh` | fine memory sweep + GC logging (the "128MB bump") |
| `analysis/scripts/run_mergepass.sh` | does more memory help once merge PASSES are at stake? |
| `analysis/scripts/run_tuning.sh` | cascade fan-in x bucket count x memory |
| `analysis/scripts/run_typecut*.sh` | type-cut buckets (measured; does not pay — dropped) |
| `run_iotest.sh` + `balloon.c` + `drop_caches.sh` | the only I/O-bound experiments |

**Config codes** (matrix): `A` stock · `B` auto-key, no bucketing · `C` bucketing, no type fix ·
`D` bucketing + auto-key · `E` everything (ideal).

**Datasets**: `test`/`big` undeclared · `typed`/`bigtyped` declared int64 · `mixbig`/`bigmixed`
multi-type · `mixclust` type-clustered · `tpcds` real TPC-DS · `tpcdsnn2` its non-nullable control.

---

## 5. Findings

### Solid (effects >20%, reproduced)
- **Stock's memory curve**: +38.4% (600MB), +80.0% (7GB undeclared), **+27.3% on real TPC-DS**.
- **Auto-detection removes it**: -2.3% (600MB), +9.6% (7GB), **-3.5% (TPC-DS)**.
- **Nullable columns**: one `?` costs stock **1.57-2.05x** on TPC-DS; we land **within 3%** of a
  non-nullable schema. No SQL++ cast escapes it (`to_double`, `CASE WHEN` both keep `ptrSize=3`).
- **Multi-type**: 1.4-1.7x faster; stock cannot express such a column at all.
- **Neighbor property**: bucketed `spreadPct=88`, 41-67 sort events; flat `spreadPct=0`, 1 event.
- **Bounded reclamation is free**: -2.9% to -5.5% (slightly *faster*), `victim-full` never fires.

### Explained
- **The "128MB bump" is a merge-pass step**, not GC. 5M rows/partition x 65B = 325MB, so 384MB holds
  a partition in ONE run (no merge) while 256MB needs two. Moves with data size. GC ruled out
  (pauses 1-100ms against 3.3s queries, uncorrelated).
- **More memory helps only while it removes merge passes.** Comparisons are ~N*log2(runSize) +
  N*log2(numRuns) = N*log2(N), a constant — memory only *moves* work between phases. At 7GB:
  8MB=163.0s, 32MB=130.3s, 512MB=134.8s, 2048MB=146.8s. Minimum at 32MB.

### Negative results (keep them in the paper)
- **Type-cut buckets do not pay** in any regime: +4.8/+15.7/+1.9% interleaved, -1.2 to +0.9%
  clustered. On clustered data ordinary size-based bucketing already yields homogeneous buckets.
- **Bucketing without the type fix is the worst configuration measured**: +46% (600MB), +115% (7GB).
  The type fix is a **precondition** for bucketing, not an independent win.
- **Stage 2's partial spill shows no benefit** over a full flush (+-1%).

---

## 6. Retracted claims (do not resurrect)

| claim | why it was wrong |
|---|---|
| "ours is 3.6-6.4% slower than declared types" | cross-experiment comparison, ~5% deploy drift. Within one run it is **-5% to +1%**. |
| "the `?` costs 2.46-3.21x on TPC-DS" | control had 3 columns vs 23 — conflated key loss with row width. Correct: **1.57-2.05x**. |
| "reclamation costs 0-5%" | 600MB workload barely spills. |
| "reclamation costs 29-40%" | `--broker periodic` takes half the REMAINING budget every 10 polls forever; budget collapsed 2047->16 frames and a 7GB sort ran in 2MB. **Bounded demands are free.** |
| "more memory never helps" | only ever sampled >=32MB at 7GB, entirely inside the single-pass regime. Below it, more memory helps 20%. |

**Pattern**: every one of these flattered a hypothesis. A number that agrees with what you expect
deserves *more* scrutiny than one that does not.

---

## 7. Reliability of the current data

Ballooning and cache invalidation were used in **only two experiments** (`iotest`, `iotest-big`).
Everything else is cache-resident CPU time on a **shared** machine.

Measured within-cell spread: 600MB matrix 5.3% median / 35.2% worst; 7GB matrix 1.2% / 23.2%;
TPC-DS 4.2% / 11.5%; bounded reclaim 7.7% / 8.6%.

| effect size | verdict |
|---|---|
| >20% | trustworthy |
| 10-20% | probably real if consistent across budgets and rounds |
| 5-10% | direction believable, magnitude not |
| <5% | not distinguishable from noise |

**For publication**: dedicated instance (AWS us-east-1 only — never us-west), balloon + cache
invalidation on every I/O claim, n>=10 for sub-10% effects, randomised arm order, report CIs.

---

## 8. Open questions

1. **Cascade fan-in and bucket size are unswept.** `--merge-fan-in 1000000` and
   `bucketCountTarget=256` came from partial evidence (`run_tuning.sh` addresses this).
2. **Neighbor property under I/O pressure** — measured cache-resident only, where interleaving
   cannot actually pay.
3. **Concurrency** — the benefit of releasing memory accrues to *another* query, so single-query
   timings can never show it. Deferred: not the paper's goal.
4. **`shrinkTo` in practice** — correct and free, but bounded demands are usually satisfied by
   *declining* frames rather than releasing them. It may only matter when a demand arrives with the
   pool already full.

---

## 9. Environment

- **nuc01** `dbis-nuc01@10.16.229.101`, 62GB RAM, 8 cores. Use **only** `~/Ameen`.
  nuc02 has 98% full disk and no JDK 21; nuc03 is in use by someone else.
- Build: `JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64`, `mvn -pl <modules> install -DskipTests`.
- **`deploy.sh` reads jars straight from `target/`** — never build while an experiment is running.
- Commits: **max 5 words, no AI attribution**. Branch `sort-ucurve-investigation`, origin is the
  personal repo `ameengee/asterixdb-sort-memory-adaptive`.
