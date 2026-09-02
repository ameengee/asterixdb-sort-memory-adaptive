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
