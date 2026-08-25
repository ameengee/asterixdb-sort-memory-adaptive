# Dr. Goetz Graefe Meeting — Speaker Notes

Deck: https://docs.google.com/presentation/d/1RLkOik7rv6sLzS3rrkNTvLfHuV8xyi2dX_RQiv9haeY/edit
Target: ~10 min / 10 slides. He can't see code, so every claim below is spoken from memory.

---

## Per-slide talking points

**1. Title.** Set context: we've been building on his external-sort / memory-adaptive work; today is current design + where we're headed.

**2. What AsterixDB is / where sort runs.** Parallel shared-nothing DBMS on the Hyracks dataflow engine. Sort is a reusable operator (ORDER BY, group-by, sort-merge join, distinct). Key fact he'll poke at: **memory is fixed at compile time**, uniform per partition (default 32 MB) — no runtime adaptation today. That's the gap we're addressing.

**3. External merge sort, two phases.** Run generation (sort-in-memory, spill sorted runs) + K-way merge. ~1024 frames of 32 KB → ~1023-way fan-in → usually a single merge pass. Fits-in-memory → no spill.

**4. In-memory sort (the meat).** Default = **stable bottom-up binary merge sort**. Quicksort variant exists but merge sort is the default and what group-by uses. **Pointer sort** — we permute a ~4-int-per-tuple index (frame id, start, end, normalized key), never the tuple bytes; tuples copied out in order only at flush. **No replacement selection** — run ≈ memory, not ~2×.

**5. Normalized keys.** *He will drill here.* **Fixed-length**, order-preserving, computed once, stored inline in the pointer index, compared as **unsigned ints**. Two-tier: normalized key first, fall back to the real comparator only on ties. Decisive (int) → comparator never runs; strings → fixed prefix, can be indecisive. Normalizer on the first sort column by default.

**6. Merge phase.** K-way merge via a **tournament / selection tree** (priority queue over run heads). Be honest if he distinguishes a Knuth loser tree from a heap — ours is a priority-queue selection tree, not strictly a loser tree.

**7. Worked example.** ORDER BY salary, 5 tuples. Show the pointer array of normalized keys, the stable reorder (Bob before Dave, both 30), bytes untouched, flush → run, spill → merge. This is the concrete thing to walk him through.

**8. U-curve puzzle.** *This is a question FOR him.* Textbook says monotone-then-flat; we see it degrade past a point. Our guesses: cache/TLB (pointer array out of L2/L3), JVM GC (on-heap buffers), OS page cache starved. Ask what actually dominates.

**9. Two directions.** Future: Powersort/Timsort-family run-adaptive merge; maybe replacement-selection-style longer runs. Done/in-progress: memory-adaptive run generation with a broker (reclaim/grant at runtime) + incremental bucketed sort that can spill a sorted prefix and keep the unsorted tail.

**10. Questions.** The five on the slide — lead with normalized keys, replacement selection, and the U-curve.

---

## Ready answers to his likely questions

- **"Fixed-length normalized keys, or something else?"** Fixed-length, order-preserving, unsigned-int compare, inline in the pointer index; comparator fallback only on ties (indecisive prefixes).
- **"Merge sort or quicksort?"** Merge sort by default (stable, bottom-up); quicksort variant available but not the default.
- **"Replacement selection?"** No — load-until-full then sort; run length ≈ memory, not ~2×. (Good opening to ask whether it's worth adding.)
- **"Stable?"** Yes — equal keys keep input order (`cmp <= 0` keeps the earlier tuple).
- **"Do you move tuples?"** No — pointer sort; bytes copied only at flush.
- **"How big are runs / fan-in?"** Run ≈ 32 MB; fan-in ≈ 1023; single-pass merge in the common case.

## Honesty flags (don't overclaim)
- The global MemoryManager.allocate being a no-op and the U-curve causes are **unconfirmed hypotheses**.
- Our broker is **simulated** (random policy) — the point is the mechanism/plumbing, not a real policy yet.
- "Loser tree" — ours is a priority-queue selection tree; clarify if he presses.
