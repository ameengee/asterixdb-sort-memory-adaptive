# Analysis: the 12 graphs

## Layout
- `scripts/run_matrix.sh`        — runs the 13 (configuration x dataset) cells at one scale
- `scripts/load_big_variants.sh` — loads `bigtyped` and `bigmixed` at 290M rows
- `scripts/make_graphs.py`       — renders graphs 1-6 (small) or 7-12 (large)
- `data/`                        — raw timings
- `figures_preliminary/`         — built from earlier complete runs, pending the full matrix

## Configurations
| code | meaning |
|------|---------|
| A | stock jar, as cloned |
| B | auto-detected key, no bucketing ("stock + our type fix") |
| C | bucketing, no type fix |
| D | bucketing + auto-detected key |
| E | bucketing + auto key + k-way ("ideal settings") |

## Datasets
`test`/`big` undeclared column · `typed`/`bigtyped` declared `int64` · `mixbig`/`bigmixed` genuinely multi-type

B and D are not run on the declared datasets: when a column is declared, the provider returns the
native normalizer and our detector never engages, so those cells would duplicate A and C exactly.

## The missing line in graphs 2, 4, 8, 10
There is no "user declares the type" line for a multi-type column because **AsterixDB cannot express
one**. The DDL builds unions only through `?` (nullable), so `k: int64 | string` is unwritable, and
declaring `k: int64` makes the double/string rows fail to load. The graphs annotate this rather than
silently dropping the line: the absence is the argument.

## Known limits, to state in the paper
- The 7GB data is **synthetic**, not TPC-DS. Relabel or generate real TPC-DS before publishing.
- Against a perfectly declared single-type column we are **3.6-6.4% slower**, not faster. The
  defensible claim is bounded harm, not universal superiority.
- Configuration C (bucketing without the type fix) is the worst thing measured (+46% slope). This is
  kept deliberately: bucketing is not independently beneficial and requires the type fix to be viable.

## Scales actually measured

| dataset | rows | on disk |
|---|---|---|
| `test` / `typed` / `mixbig` | 10M | ~0.6-0.9 GB |
| `big` | 290M | 6.8 GB |
| `bigtyped` | 290M | 6.6 GB |
| `bigmixed` | 290M | 7.6 GB |

**Graphs 7-12 are NOT the I/O-bound result.** `run_matrix.sh` applies no page-cache balloon, and the
machine has ~26 GB of cache against ~7 GB per dataset, so each query is largely cache-resident.
They measure how the configurations behave at 29x the data — more comparisons per tuple, deeper
runs — not what happens when reads reach the device.

The I/O-bound result is separate (`results/iotest-big/`, run with a 40 GB balloon and a per-query
`fadvise` cache drop): stock +54.3% across 32MB->2GB, ours +11.6%, speedup 1.90x -> 2.63x. Keep the
two framed distinctly; merging them would overstate what either measured.

## Result summary (600MB, n=6 per cell)

| graph | question | answer |
|---|---|---|
| 1 | U-curve on stock, single type | +38.4% undeclared; -6.2% declared; **-2.3% auto-detected** |
| 2 | multi-type on stock | 1.44-1.46x faster with detection, but slope stays +23.6% |
| 3 | bucketed, single type | +46.3% without the type fix; **+0.6%** with it |
| 4 | bucketed, multi-type | +46.1% -> +32.3% |
| 5 | as-cloned vs our best, multi-type | **1.50-1.63x faster** |
| 6 | where stock is strongest | **-5.0% to +1.0%** (parity or better) |

Two results to state plainly rather than bury:
- The multi-type SLOPE is not fixed (+23.6% / +32.3%). A fixed-width key cannot be injective over
  strings, so ties fall back to the comparator. Type-cut buckets is the intended fix.
- Bucketing WITHOUT the type fix is the worst configuration measured (+46%), worse than plain stock.
  The type fix is a precondition for bucketing, not an independent contribution.
