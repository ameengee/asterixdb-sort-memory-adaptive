
# Adaptive Sort Algorithm — Design Plan

> **Status:** design/plan. No code yet. Companion to `sort_operator_deep_dive.md`
> (how the sort works today), `sort_memory_curve_theories.md` (unconfirmed U-curve
> hypotheses), and `demo_memory_adaptive_commands.md` (the broker-simulation demo).
>
> **Goal:** make the in-memory sort *algorithm* itself memory-adaptive and better at
> resource utilization — building on the simulated broker we already have.

---

## 0. Scope & hard constraints

- **The simulated broker stays in `AbstractExternalSortRunGenerator`.** We are not
  moving it and not building a real broker.
- **We do NOT touch `MemoryManager`** (the global accountant stays as-is). All work is
  inside the sort operator / frame sorter.
- **No second thread.** AsterixDB will not let us claim another thread. "Async" here
  means **single-thread cooperative interleaving** (FastAPI-style: do a bounded chunk of
  work at each `nextFrame`), *not* a background worker thread.
- **We do NOT claim this fixes the U-curve.** The GC / page-cache / cache-locality
  explanations in `sort_memory_curve_theories.md` are **unconfirmed hypotheses** — our
  best guesses, not established facts. We don't actually know the cause of the U-curve.
  These improvements are worthwhile **on their own merits** (overlap, cache-efficiency,
  graceful adaptivity); any effect on the U-curve is speculative and must be *measured*,
  never assumed.

---

## 1. What the in-memory sort does today (the target)

Per run, `AbstractExternalSortRunGenerator` + `AbstractFrameSorter` do:

1. **Accumulate**: every `nextFrame` `arraycopy`s the incoming frame into the pool
   (`VariableFramePool`). No sorting happens yet.
2. **One big sort**: at spill or `close()`, `sort()` builds one `tPointers` array over
   *all* frames and sorts it in a single shot (bottom-up merge, or quicksort).
3. **Flush**: walk the sorted `tPointers`, copy tuples out in order to a run file.

Two consequences we want to fix:

- **No CPU/accumulation overlap.** The sort does zero CPU work until every frame is in,
  then does one big CPU spike, then one big write. (Note: the sort's "load" is a cheap
  in-memory `arraycopy`; the disk read is the upstream scan's cost. What we're attacking
  is the *deferred CPU spike*, not the sort doing disk reads.)
- **Victimization is all-or-nothing.** On a victim signal we must flush the *entire*
  accumulated batch to a single run — slow, and it throws away the chance to keep the
  unsorted tail in memory.

---

## 2. Goals (each stands on its own, independent of the U-curve)

1. **Overlap CPU with data arrival** — spread sort work across the accumulation window so
   there's no end-of-input CPU spike, and so by the time input ends most work is done.
2. **Cache-conscious sorting** — sort in chunks sized to the CPU cache (the AlphaSort idea,
   ref [4] in the MASORT paper) — standard good practice for sort performance.
3. **Graceful shrink** — on victimization, give memory back *cheaply* by spilling
   already-sorted data as a ready-made run, keeping the unsorted tail in memory.
4. **Graceful grow** — accept more memory by folding new sorted buckets in, without
   re-doing work.

---

## 3. The design in one paragraph

Turn the in-memory sort from *"accumulate everything → one big sort → spill one run"*
into an **incremental, cache-conscious, single-threaded pipelined sort that maintains a
small set of already-sorted in-memory runs.** Grow = accept frames, sort them into
cache-sized buckets, fold buckets into runs. Shrink (victim) = spill a large already-sorted
run (sequential write, ~zero CPU) and keep the unsorted tail. All of it rides on the
existing `tPointers` model, so **tuple data never moves until flush** — the incremental
sort and the progressive merge operate on `tPointers` sub-arrays.

---

## 4. The four mechanisms and how they interlock

### 4.1 Cache-sized buckets (not a fixed *number* of buckets)
- A **bucket** = a set of frames whose combined data + their `tPointers` slice fits in a
  target cache size (start with L2; make it a tunable). Sort that bucket's `tPointers`
  slice while it's cache-resident.
- **Refinement:** size it so both the *pointers* and the referenced *tuple data* fit in
  cache. Tier-1 comparisons already read the contiguous `tPointers` (cache-friendly); the
  tier-2 tie-break chases into frame data scattered across the pool (cache-hostile). A
  cache-sized bucket keeps *both* hot.
- **Tradeoff:** small buckets ⇒ many buckets ⇒ a wide k-way final merge whose heap itself
  falls out of cache at large k. Bucket size vs. final-merge fan-in pull against each
  other — which is exactly what 4.3 manages.
- **Honesty note:** cache-conscious sizing is good practice regardless. *If* cache-locality
  contributes to the U-curve it may also help there — but that is a hypothesis to measure,
  not the reason we're doing it.

### 4.2 Incremental sort (single-thread cooperative — the "async" model)
- Do a **bounded chunk of sort work at each `nextFrame`**: insert the frame; if a bucket
  just filled to cache-size, sort that bucket now; occasionally advance a merge (4.3).
- This is the FastAPI-style single-thread model adapted to Hyracks' push protocol:
  `nextFrame` is the "await data" point, and between awaits we do a slice of pending work.
  **No second thread, no async I/O primitives.**
- **What overlap we actually get (verified — see §8.1):** The sort's upstream is a
  **separate task on a separate thread**, delivering frames through a **buffered channel
  filled asynchronously** by the network/comm layer (`InputChannelFrameReader` gets
  `notifyDataAvailability` from `NetworkInputChannel` / `MaterializedPartitionInputChannel`,
  not from the operator thread). So:
  - **We get real cross-thread overlap for free:** while the sort processes frame N, the
    upstream task + comm thread produce and buffer frame N+1. `nextFrame` only *blocks*
    (`InputChannelFrameReader.canGetNextBuffer` → `wait()`) when the sort drains the buffer
    faster than upstream fills it. Doing a bounded slice of sort work per `nextFrame` means
    that CPU work overlaps the upstream's next-frame production automatically.
  - **We do NOT need a second thread, and we do NOT need a non-blocking read.** The only
    thing a poll API would add is using *starvation-block* time to do pending sort work —
    but if we sort incrementally, a starvation block means upstream is the bottleneck and we
    have no pending work anyway, so parking is correct. Not worth touching Hyracks comm
    classes (and would violate the "changes stay in the sort operator" constraint).

### 4.3 Balanced progressive merge (NOT merge-into-one-growing-run)
- Fold sorted buckets into larger sorted runs **in a balanced/geometric cascade**: merge
  *k* equal-sized runs into one bigger run (size-B buckets → merge k into a size-kB run →
  merge k of those → …), like LSM leveling.
- **Critical correction:** do **not** merge each new bucket into a single ever-growing run
  — that is **O(n²/bucketSize)**. Balanced merging keeps total work at **O(n log n)** while
  still yielding a *small number of large runs*.
- In the pointer model, "merging two sorted runs" = merging their `tPointers` sub-arrays
  into one sorted sub-array; the underlying frames stay put, `frameId` in each pointer
  keeps the reference. So progressive in-memory merge is cheap (pointer moves only).
- **Eager vs. lazy (a real tradeoff):**
  - *Eager* (merge continuously) → a large run is always spill-ready → fast victim response,
    but wasted CPU if no victim ever comes (in a fully-in-memory sort you'd ideally merge
    once, at the end).
  - *Lazy* (keep buckets separate; only fold into a run when a victim signal arrives or
    memory gets tight) → minimal overhead, slightly slower victim response.
  - **Recommendation:** start lazy/on-demand (pay the merge only when it buys something);
    revisit if victim-response latency turns out to matter.

### 4.4 Shrink primitive: spill the largest sorted run, keep the unsorted tail
- On a victim signal, spill an **already-sorted** run: walk its `tPointers` sub-array,
  write tuples in order, free those frames. **Zero sort CPU** — it was already sorted.
- Keep the **unsorted staging** in memory (you still have to sort it; don't waste the
  write). Rationale: sorted data is a run for free; unsorted data spilled is wasted work.
- Spilling the *largest* run (4.3 keeps one around) frees the most memory per spill and
  keeps the total **run count** low — which keeps the final external merge single-pass.

### How they interlock
Cache-sized buckets (4.1) give fast local sorts but many runs → balanced progressive merge
(4.3) controls the run count and keeps a large run spill-ready → spill-largest-sorted-run
(4.4) is the cheap shrink that (4.3) enables → incremental cooperative execution (4.2)
spreads it all across accumulation. One machine, four parts.

---

## 5. Constraints & correctness

- **Stability.** `ORDER BY` expects stable output. Buckets must stay in input order and
  every merge must take the left element on ties (the current code already does `cmp <= 0`
  → left). Preserve this through bucketing and progressive merge.
- **Single-pass final merge.** AsterixDB's external merge is single-pass up to ~1023 runs;
  keeping run count low (4.3/4.4) protects that. Multi-pass merges are "virtually never
  needed" (MASORT §2) so we don't optimize for them.
- **Variable-length tuples.** "Fits in L2" is fuzzy with variable-size tuples; size buckets
  by accumulated bytes, not tuple count.
- **Data doesn't move until flush.** Keep the invariant that sorting/merging permute
  `tPointers` only; tuple bytes are copied exactly once, at flush. This is what makes the
  whole thing cheap and fits `AbstractFrameSorter` as-is.

---

## 6. What this explicitly does NOT claim

- It does **not** claim to fix or explain the U-curve. The cause is unknown; the theories
  file is hypotheses.
- It does **not** address GC pressure or page-cache eviction — and we are **not** asserting
  those are real problems here; they're unconfirmed guesses. Cache-sized buckets don't
  shrink total heap, so nothing here is expected to change GC behavior.
- Any performance claim (overlap benefit, cache benefit, victim-cost reduction) must be
  **measured**, not assumed. Design the prototype so each mechanism can be toggled and
  benchmarked independently.

---

## 7. Staged build plan (de-risking order)

1. **[DONE]** **Incremental cache-sized bucket sort + merge-at-spill (single thread, no async).**
   Sort each cache-sized bucket as it fills (on the operator thread); on flush/victim,
   k-way merge the sorted buckets into the run. Spreads the CPU spike, makes spilling
   cheaper, gives cache benefit. Biggest bang, lowest risk. *No threads, no async.*
   Landed in `AbstractFrameSorter` (buildFramePointers / sealBucket / sortBucketSlice /
   mergeBuckets); verified exact-sorted across budgets.
2. **[DONE]** **Shrink primitive (4.4).** Spill-largest-sorted-run + keep unsorted tail, wired to the
   existing victim path in `nextFrame`. Landed as `spillSortedKeepUnsorted` +
   `spillSortedKeepUnsortedToRun`; verified exact-sorted.
3. **[DONE]** **Balanced progressive merge (4.3).** As each bucket seals it becomes a level-0 run;
   whenever the two newest runs are the same size we merge them (a binary "merge-when-equal"
   cascade, like LSM leveling). This keeps only ~log(#buckets) runs around, spreads the merge
   work across accumulation, and makes victim spills cheaper (few runs left to merge). Chose
   *eager* over the originally-suggested lazy: eager and lazy do the **same** total merge work
   (both walk the same binary merge tree), so eager costs nothing extra and wins on victim
   latency. Landed in `sealBucket` (the cascade) + `mergeTwoRuns`.
4. **Cooperative overlap tuning (4.2).** Tune how much sort work per `nextFrame`; if the
   reader can be polled non-blocking, exploit true I/O overlap (open question 8.1).
5. **Tune bucket size** and benchmark each mechanism independently (against a static-memory
   baseline and the current code).

---

## 8. Open questions / things to measure

1. **Non-blocking reads? — RESOLVED (no poll API needed).** There is no non-blocking read:
   `IFrameReader.nextFrame` is blocking and `InputChannelFrameReader.canGetNextBuffer()` does
   `wait()` when `availableFrames <= 0`; a grep of the reader/collector/comm path found no
   `tryNextFrame`/`isFrameReady`/`available()`. **But we don't need one.** The upstream runs
   as a separate task/thread and frames are delivered via a buffered channel filled
   asynchronously by the comm layer (`notifyDataAvailability` from `NetworkInputChannel` /
   `MaterializedPartitionInputChannel`). So incremental-sort-per-`nextFrame` already overlaps
   the sort's CPU with upstream production — zero changes outside the sort operator, no second
   thread. (Details in §4.2.)
2. **Bucket size.** What target (L2? a fraction of L2? L3?) actually minimizes sort time
   here, given variable-length tuples and the normalized-key fast path?
3. **Eager vs. lazy merge.** How latency-sensitive is victim response in practice? Decides
   4.3's policy.
4. **Does incremental sorting cost more total CPU** than one big sort (more, smaller
   sorts + merges vs. one pass)? Measure the overhead vs. the overlap/victim benefits.
5. **Baseline U-curve, honestly.** Re-measure the U-curve with this design and *without*
   assuming a cause — does better overlap/cache change the curve at all? Report whatever
   happens, including "no change."

---

## 9. Where it lands in the code (for when we build)

| Concern | File / hook |
| :-- | :-- |
| Broker signals (victim / grant) — **stays here** | `AbstractExternalSortRunGenerator.nextFrame` |
| Incremental bucket sort, progressive merge, `tPointers` | `AbstractFrameSorter` (+ `FrameSorterMergeSort`) |
| Cache-sized bucket boundary (bytes) | new logic in `AbstractFrameSorter.insertFrame` / `sort()` |
| Spill a specific sorted run (not the whole batch) | new path alongside `flushFramesToRun` |
| In-memory frames, lazy allocation (not thread-safe) | `VariableFramePool` / `VariableFrameMemoryManager` |
| Final external merge (keep single-pass) | `AbstractExternalSortRunMerger` (unchanged) |

---

*Reminder for the writeup: the value proposition is overlap + cache-efficiency + graceful
adaptivity. The U-curve is a separate, unresolved question — we improve the sort because
these are good properties, and we measure the U-curve honestly without assuming we've
touched its cause.*
