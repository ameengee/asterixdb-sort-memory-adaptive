# Why More Sort Memory Helps — Then Hurts (U-Shaped Curve)

> **Status:** hypotheses / research notes. Parked for later — revisit after we've
> walked through `sort_operator_deep_dive.md` and understand the code fully.
>
> **Observation to explain:** In AsterixDB, giving the sort operator more memory
> makes it faster up to a point, then progressively *slower*. Not just diminishing
> returns — an actual upward turn in wall-clock time.

---

## Verified code facts these theories rest on

- **All sort memory is on the Java heap.** `FrameManager.allocateFrame(int)` uses
  `ByteBuffer.allocate(bytes)` (heap), NOT `allocateDirect`
  (`hyracks-control-nc/.../resources/memory/FrameManager.java:56`). This is why the
  whole codebase can call `buffer.array()`. → GC applies to every sort byte.
- **Merge sort doubles pointer memory:** `tPointers` + `tPointersTemp`
  (`FrameSorterMergeSort.getRequiredMemory`, `AbstractFrameSorter.sort:167`).
- **Total comparison work is ~`N log N` regardless of memory split** (see the
  user's own `sorting_efficiency.md`). So the U-shape is NOT an algorithmic-work
  effect — it's a memory-hierarchy / systems effect layered on constant work.
- **Merge fan-in scales with memory:** `maxMergeWidth = framesLimit - 1`
  (`AbstractExternalSortRunMerger.java:75`).

---

## The core framing

```
total_time  =  (I/O benefit: shrinking AND floored)  +  (costs: growing monotonically)
```

- **Benefit is bounded.** Memory removes merge passes: passes ≈ `log_M(N/M)`, which
  falls fast then hits a floor of **1 pass**, then **0 passes** (fully in-memory
  fast path, `AbstractSorterOperatorDescriptor.java:184`, `runs.isEmpty()`). You
  can't go below zero. Past that point, extra memory buys nothing on I/O.
- **Costs keep growing** (below). Once the benefit flattens, growing costs win →
  the curve turns up. The optimum is the crossover point.

---

## The cost mechanisms (ranked)

### 1. GC pressure — top suspect for AsterixDB specifically
Every frame + the big `int[] tPointers`/`tPointersTemp` are live Java heap. As
memory M grows: `new int[tupleCount * ptrSize]` becomes a huge old-gen/humongous
allocation; live set grows → more frequent, longer GC pauses; near the heap ceiling
→ full-GC thrashing that can dominate runtime.
**Signature:** GC pause time / allocation rate tracks the slowdown (`-Xlog:gc`).

### 2. CPU cache / TLB locality collapse in the in-memory sort
When a run fits in LLC, compares/swaps are cheap. As runs grow with M:
- `FrameSorterMergeSort.sort` streams the whole `tPointers` array `log₂(B)` times,
  ping-ponging with `tPointersTemp`.
- Tier-2 compare (`AbstractFrameSorter.compare:255-281`) touches real tuple bytes
  scattered across the whole pool → random access over M bytes → DRAM/TLB misses
  once M ≫ LLC.
Per-comparison cost rises with M while I/O savings shrink.
**Signature:** LLC-miss / TLB-miss counters (`perf stat`); persists even when the
whole input fits in memory (no runs).

### 3. Page-cache eviction (OS memory tug-of-war)
Spilled runs are written then re-read almost immediately; if the OS page cache
holds them, that I/O is nearly free. Sort heap and page cache share physical RAM,
so more sort memory **steals cache from the run files it just wrote** → reads hit
real disk. Worst case: swapping → cliff.
**Signature:** actual device I/O vs. Hyracks logical run I/O; `vmstat`/swap.

### 4. Over-wide single-pass merge
`maxMergeWidth = framesLimit - 1`. Large M → huge fan-in K in one pass: per output
tuple costs `log K` in the tournament tree (`ReferencedPriorityQueue`); round-robin
reads of K buffers scattered on the heap (poor locality); `prepareFrames` inflates
each read buffer → many concurrent large random reads across K file handles instead
of fewer sequential streams. A moderate fan-in with 2 passes can beat one giant
fan-in. (Optimal merge width is NOT "as wide as possible.")
**Signature:** number of merge passes (already logged in
`AbstractExternalSortRunMerger`) + per-pass timing.

### 5. Minor bookkeeping that scales with buffer count
`VariableFramePool.reset()` does `Collections.sort` of the buffer list every run;
`mergeExistingFrames`/`binarySearchUnusedBuffer` scale with buffer count. Small but
nonzero, grows with M.

---

## Current bet

Descending-then-ascending shape ≈ **#1 (GC) + #3 (page cache)** producing the
actual upward turn, with **#2 (cache locality)** setting the smooth
diminishing-returns slope, and **#4** contributing for very wide merges.
Rationale: GC and page-cache can produce a real upward turn; cache-locality alone
mostly just flattens the gains.

---

## Cleanest isolating experiment

Force the **fully-in-memory** case (input smaller than M) and keep growing M. Zero
I/O, zero merge there — so if it *still* slows down, you've isolated pure CPU-cache
+ GC effects from I/O/page-cache effects. Then re-introduce spilling to measure #3/#4.

Per-hypothesis, sweep M and record the signature listed under each mechanism above.
