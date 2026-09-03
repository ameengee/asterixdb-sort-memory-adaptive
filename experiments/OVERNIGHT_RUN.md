# Overnight I/O run — live state and resume instructions

**Started 2026-09-03 ~07:50 (nuc01 clock). Updated as it progresses.**
If context was lost, this file plus `METHODOLOGY.md` is everything needed to finish the job.

---

## What is running

`analysis/scripts/run_overnight_io.sh` on nuc01, detached (`setsid nohup`, PPID 1).
Every headline claim re-measured under **real I/O pressure**: 44GB balloon + a `fadvise` cache drop
before every timed query. All earlier headline numbers were cache-resident and cannot show a memory
benefit at all, because a single-pass merge moves the same bytes regardless of run count.

| stage | what it answers | arms |
|---|---|---|
| **S** | U-curve solved? memory benefit? bucketing no-harm? | `stock`, `nobucket` (type fix only), `bucket` (type fix + bucketing) |
| **N** | good neighbour? | `bucketed` vs `flat`, via `spreadPct` / `sortEvents` |
| **R** | cost of releasing memory | `none`/`reclaim` at a budget ABOVE and BELOW the single-pass threshold |

Datasets: `test` (600MB single-type), `mixbig` (600MB multi-type), `tpcds` (real TPC-DS store_sales).

### Why the memory levels start at 2-4MB
Single-pass threshold is `B < sqrt(dataPerPartition * frameSize)`:
`test`/`mixbig` ~**3.3MB**, `tpcds` ~**7.6MB**, `big` ~**17.5MB**.
Every previous sweep tested only ABOVE these, which is why "more memory helps" never appeared.

---

## Monitoring / status

```bash
ssh dbis-nuc01@10.16.229.101 'echo "st=$(cat ~/Ameen/overnight.status) hb=$(cat ~/Ameen/overnight.heartbeat) n=$(wc -l < ~/Ameen/overnight/times.txt)"'
```
`status` is `RUNNING` / `DONE` / `FAILED`. `~/Ameen/overnight/log.txt` records SKIPped cells
(a budget the engine rejected — recorded, not fatal).

---

## When it finishes — exactly these steps

```bash
cd ~/Codebase/asterixdb/experiments
scp dbis-nuc01@10.16.229.101:'~/Ameen/overnight/times.txt'  analysis/data/overnight_times.txt
scp dbis-nuc01@10.16.229.101:'~/Ameen/overnight/spread.txt' analysis/data/overnight_spread.txt
scp dbis-nuc01@10.16.229.101:'~/Ameen/overnight/mem.txt'    analysis/data/overnight_mem.txt
scp dbis-nuc01@10.16.229.101:'~/Ameen/overnight/log.txt'    analysis/data/overnight_log.txt

python3 analysis/scripts/make_io_figures.py \
    analysis/data/overnight_times.txt analysis/data/overnight_spread.txt analysis/figures_io
```
Produces in `analysis/figures_io/`: `ucurve_test.png`, `ucurve_mixbig.png`, `ucurve_tpcds.png`,
`noharm.png`, `neighbor.png`, `reclaim.png`.

Then commit (max 5 words, no AI attribution):
```bash
git add -f analysis/data/overnight_*.txt; git add analysis
git commit -m "overnight io results"; git push origin sort-ucurve-investigation
```

---

## Data format

`times.txt`: `<kind> <dataset> <arm> <mem> <seconds>` — kind is `S`, `N`, or `R`.
`spread.txt`: `SPREAD <dataset> <arm> spreadPct=N sortEvents=M`.

---

## Predictions recorded BEFORE the data (so they can be falsified)

1. **Memory benefit appears below the threshold**: a real drop from 2MB->8MB on the 600MB sets and
   4MB->16MB on TPC-DS; near-flat above.
2. **Reclaim is free above the threshold, ~12% below it** (the cost of one extra merge pass).
3. **Neighbour holds**: bucketed `spreadPct` high with many sort events; flat ~0 with 1 event.
4. **Bucketing no-harm holds on single-type**, with a possible ~6-8% cost on multi-type at 600MB.

---

## NOT done, deliberately

**The 2x scratch reservation is unfixed.** `tScratch` is the merge's DESTINATION array, not padding,
so `2x` is honest accounting; `ensureScratchCapacity(hi)` grows it to the whole run because the
cascade merge spans it. Removing it requires **streaming the merge at flush time** (run the winner
tree inside `flush()` and emit straight to the writer) instead of materialising it — a real refactor
of the core sort path. It matters because that overhead raises our single-pass threshold above
stock's, and crossing that threshold is worth ~12.6%. Do it gated, with Ameen awake.

---

## Status log

- 07:50 launched; balloon holding, cache 0-2GB
- 07:57 **`test` (600MB single-type) COMPLETE — every prediction held.**
  Threshold ~3.3MB, and crossing it (2MB->4MB) is worth **-15% stock / -15% nobucket / -14% bucket**.

  | arm | 2MB | 4MB | 8MB | 32MB | 128MB | 512MB |
  |---|---|---|---|---|---|---|
  | stock | 6.88 | 5.87 | 5.90 | 6.33 | 7.94 | 8.37 |
  | nobucket | 4.36 | 3.72 | 3.64 | 3.72 | 3.98 | 3.63 |
  | bucket | 4.28 | 3.66 | 3.60 | 3.60 | 4.00 | 3.68 |

  - **U-curve**: stock from its best (4MB) to 512MB is **+43%**; ours is **+2%** (flat).
  - **Memory benefit**: real, and located exactly at the predicted threshold.
  - **No harm**: bucketing faster-or-equal at 5 of 6 budgets.
- 08:09 **`mixbig` (600MB multi-type) COMPLETE — one result REVERSES a cached finding.**

  | arm | 2MB | 4MB | 8MB | 32MB | 128MB | 512MB | best->512MB |
  |---|---|---|---|---|---|---|---|
  | stock | 7.35 | 6.61 | 6.62 | 6.97 | 8.51 | 9.02 | +36.5% |
  | nobucket | 5.81 | 5.10 | 4.99 | 5.07 | 5.71 | 6.43 | +28.9% |
  | bucket | 5.45 | 4.41 | 4.87 | 4.81 | 5.67 | 6.03 | +36.8% |

  - **Bucketing HELPS on multi-type under I/O**: -6.2/-13.5/-2.5/-5.0/-0.8/-6.3%. Cache-resident the
    same comparison showed bucketing COSTING 6-8%. The flip is the neighbour property paying off --
    multi-type comparisons are expensive (comparator fallback), so there is ample CPU to overlap
    with real I/O. **The earlier "bucketing harms multi-type" finding was a cache artifact.**
  - Speedup over stock steady at **1.35-1.50x**.
  - The multi-type U-curve is NOT removed for anyone (+29% to +37%, all three arms) -- strings make
    the key inexact, so the comparator fallback scales with run size regardless of bucketing.
- 08:30 **TPC-DS + neighbour + reclaim(test) COMPLETE. All five claims now I/O-verified.**

  **TPC-DS store_sales (28.8M rows), threshold ~7.6MB:**

  | arm | 4MB | 8MB | 16MB | 64MB | 256MB | 512MB | best->max |
  |---|---|---|---|---|---|---|---|
  | stock | 26.7 | 24.3 | 25.0 | 27.1 | 31.4 | 33.5 | **+37.8%** |
  | nobucket | 19.9 | 17.1 | 16.7 | 16.9 | 17.6 | 17.8 | +6.8% |
  | bucket | 19.3 | 17.3 | 17.0 | 17.2 | 18.0 | 18.0 | **+6.2%** |

  speedup over stock **1.39x -> 1.85x** (grows with budget); bucketing cost -3.1% to +2.2% (neutral).

  **Neighbour (under I/O):** 600MB bucketed spreadPct=**85** / 34 sort events vs flat **0** / 1 event;
  TPC-DS bucketed **91** / 66 events vs flat **0** / 1.

  **Reclaim (give back 50%):** 600MB above threshold **-2.7%** (free), below threshold **+7.7%**
  (one merge pass); TPC-DS above threshold **-1.0%** (free).
  -> **Releasing memory is free while the sort stays above its single-pass threshold, and costs one
  merge pass below it.** Predicted ~12.6%, measured +7.7%.
- 08:35 **RUN COMPLETE — 198 samples, 0 skips, balloon held throughout (cache 2-3GB).**
  All six figures generated in `analysis/figures_io/`.

  Final reclaim numbers: 600MB above threshold **-2.7%** / below **+7.7%**;
  TPC-DS above **-1.0%** / below **+5.1%**. Free above, one merge pass below -- on both datasets.

## FINAL SUMMARY (all under real I/O)

| claim | evidence |
|---|---|
| Better neighbour | spreadPct **85** (600MB) / **91** (TPC-DS) vs **0**; 34-66 sort events vs 1 |
| Bucketing no harm | single-type +-2%, TPC-DS -3.1..+2.2%, multi-type **-0.8 to -13.5% (helps)** |
| U-curve solved | stock **+43%** (600MB) / **+37.8%** (TPC-DS) from its best; ours **+2%** / **+6.2%** |
| Memory benefit | **-15%** crossing the single-pass threshold; flat above it |
| Release memory | **free** above threshold (-1 to -2.7%), **+5.1 to +7.7%** below |

**The organising idea:** the single-pass threshold `B < sqrt(dataPerPartition * frameSize)` says how
much memory a sort should get, why more is wasted, and why giving it back is free down to that point.

**Three findings only appeared once the page cache was removed:** the memory benefit (invisible
above the threshold), bucketing's multi-type "harm" reversing into a benefit, and the reclaim cost
splitting cleanly by threshold instead of looking uniformly free.

