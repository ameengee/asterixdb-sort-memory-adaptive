# AsterixDB / Hyracks Sort Operator — Byte-by-Byte Deep Dive

> A complete walkthrough of how sorting works in AsterixDB, from an `ORDER BY`
> clause in a SQL++ query down to the individual bytes moved in memory and on
> disk. Written as a code-navigation companion: every subsection lists the
> **file** and **line numbers** so we can open the code side-by-side.
>
> Scope: the *external merge sort* is the workhorse. Variants (top-K, heap sort,
> sort-based group-by, in-memory/micro sort) are covered in their own sections.

---

## 0. The 60-second mental model

AsterixDB never assumes the data fits in memory. The sort operator is a classic
**two-phase external merge sort**:

1. **Phase 1 — Run generation (the "Sort" activity).**
   Input frames stream in. They are packed into a fixed memory budget
   (default **32 MB**, i.e. **1024 frames** of 32 KB). When the budget fills,
   the in-memory tuples are sorted and flushed to disk as one **sorted run**
   (a temp file). Memory is reset and we keep going. Result: a list of sorted
   runs on disk (plus possibly one last un-flushed batch still in memory).

2. **Phase 2 — Merge (the "Merge" activity).**
   The sorted runs are merged with a **K-way merge** driven by a
   loser/tournament-style priority queue. If there are more runs than fit in the
   memory budget at once, merging happens in **multiple passes** (merge groups of
   runs into bigger runs, repeat) until a single final merged stream is produced.

Key trick for speed: the actual variable-length tuple bytes are **never moved**
during sorting. Only small fixed-size **tuple pointers** (`int[]`) are permuted,
and each pointer carries a **normalized key** prefix so most comparisons are a
cheap integer compare with no pointer-chasing.

The two phases are wired as two Hyracks *activities* joined by a **blocking
edge** — Phase 2 cannot start until Phase 1 has fully finished.

---

## Table of contents

1. [Where sort lives — the module map](#1-where-sort-lives--the-module-map)
2. [Call path: from `ORDER BY` to a running operator](#2-call-path-from-order-by-to-a-running-operator)  *(pending agent)*
3. [Memory budget: how bytes become "frames"](#3-memory-budget-how-bytes-become-frames)
4. [The frame byte format (physical tuple layout)](#4-the-frame-byte-format-physical-tuple-layout)
5. [The operator descriptor & its two activities](#5-the-operator-descriptor--its-two-activities)
6. [Phase 1: run generation](#6-phase-1-run-generation)
7. [The frame sorter: tuple pointers & normalized keys](#7-the-frame-sorter-tuple-pointers--normalized-keys)
8. [The two in-memory sort algorithms](#8-the-two-in-memory-sort-algorithms)
9. [The memory manager (buffer pool)](#9-the-memory-manager-buffer-pool)
10. [Spilling a run to disk](#10-spilling-a-run-to-disk)
11. [Phase 2: the multi-pass merge](#11-phase-2-the-multi-pass-merge)
12. [The K-way merge heap](#12-the-k-way-merge-heap)
13. [Variants: top-K, heap sort, group-by, micro/in-memory](#13-variants)  *(pending agent)*
14. [End-to-end trace of one tuple](#14-end-to-end-trace-of-one-tuple)
15. [Glossary & where each budget number comes from](#15-glossary)

---

## 1. Where sort lives — the module map

There are two relevant Maven super-modules:

- **`hyracks-fullstack/`** — the low-level parallel dataflow runtime (Hyracks) +
  the query-algebra layer (Algebricks). **All the actual sort machinery is here.**
- **`asterixdb/`** — the AsterixDB layer on top: SQL++ parser, metadata,
  configuration (this is where the 32 MB default is defined).

The core external-sort engine is one package:

```
hyracks-fullstack/hyracks/hyracks-dataflow-std/src/main/java/org/apache/hyracks/dataflow/std/sort/
├── AbstractSorterOperatorDescriptor.java   # operator + 2 activities (sort, merge)
├── ExternalSortOperatorDescriptor.java     # the concrete full external sort
├── InMemorySortOperatorDescriptor.java     # sort with no spilling
├── TopKSorterOperatorDescriptor.java       # ORDER BY ... LIMIT k
│
├── IRunGenerator.java                      # Phase-1 interface
├── AbstractSortRunGenerator.java           # Phase-1 skeleton (flush/close)
├── AbstractExternalSortRunGenerator.java   # builds frame sorter + buffer mgr
├── ExternalSortRunGenerator.java           # concrete: run files on disk
├── HeapSortRunGenerator.java               # top-K run generation (heap)
├── HybridTopKSortRunGenerator.java         # adaptive top-K
│
├── ISorter.java / IFrameSorter.java        # in-memory sorter interfaces
├── AbstractFrameSorter.java                # tuple pointers, normalized keys, compare
├── FrameSorterMergeSort.java               # bottom-up merge sort of pointers
├── FrameSorterQuickSort.java               # 3-way quicksort of pointers
├── ITupleSorter.java / TupleSorterHeapSort.java  # heap-based sorter (top-K)
│
├── AbstractExternalSortRunMerger.java      # Phase-2 multi-pass driver
├── ExternalSortRunMerger.java              # concrete merger
├── RunMergingFrameReader.java              # the K-way merge itself
│
├── Algorithm.java                          # enum { MERGE_SORT, QUICK_SORT }
└── util/
    ├── GroupVSizeFrame.java                # multi-frame read buffer
    ├── GroupFrameAccessor.java             # parse several frames as one
    └── ...
```

Supporting cast in sibling packages:

| Concern | File |
| :-- | :-- |
| Buffer pool (bytes) | `.../dataflow/std/buffermanager/VariableFramePool.java` |
| Logical→physical frame mgr | `.../dataflow/std/buffermanager/VariableFrameMemoryManager.java` |
| Free-slot placement policy | `.../dataflow/std/buffermanager/FrameFreeSlotLastFit.java` |
| Merge priority queue | `.../dataflow/std/util/ReferencedPriorityQueue.java` |
| Merge queue entry | `.../dataflow/std/util/ReferenceEntry.java` |
| Frame byte format (read) | `hyracks-dataflow-common/.../comm/io/FrameTupleAccessor.java` |
| Frame byte format (write) | `hyracks-dataflow-common/.../comm/io/FrameTupleAppender.java` + `AbstractFrameAppender.java` |
| Frame constants/offsets | `hyracks-api/.../comm/FrameConstants.java`, `FrameHelper.java` |
| Variable-size frame | `hyracks-api/.../comm/VSizeFrame.java` |
| Run file I/O | `hyracks-dataflow-common/.../io/RunFileWriter.java`, `RunFileReader.java`, `GeneratedRunFileReader.java` |
| Normalized-key compare | `hyracks-dataflow-common/.../utils/NormalizedKeyUtils.java` |
| Sort memory default | `asterixdb/asterix-common/.../config/CompilerProperties.java` |
| Frames-from-bytes math | `asterixdb/asterix-common/.../config/OptimizationConfUtil.java` |
| Physical operator | `hyracks-fullstack/algebricks/.../physical/StableSortPOperator.java` (+ `Abstract...`, `MicroStableSortPOperator.java`) |

---

## 2. Call path: from `ORDER BY` to a running operator

Seven stages take a SQL++ `ORDER BY` down to a live `ExternalSortOperatorDescriptor`.
The through-line to keep your eye on is the **memory budget**: 32 MB → 1024 frames →
`framesLimit` argument of the descriptor.

### Stage 1 — Parse/translate: `ORDER BY` → logical `OrderOperator`

**File:** `asterixdb/asterix-algebra/.../translator/LangExpressionToPlanTranslator.java`

- `visit(OrderbyClause oc, ...)` at **line 1496**: `new OrderOperator()` (line 1499).
- Lines 1505–1513 convert each order expression + ASC/DESC + null-order modifier
  into entries via `addOrderByExpression(...)`.
- Lines 1515–1519 attach optional hints as annotations
  (`OperatorAnnotations.CARDINALITY`, `MAX_NUMBER_FRAMES`).

### Stage 2 — The logical operator: `OrderOperator`

**File:** `hyracks-fullstack/algebricks/.../operators/logical/OrderOperator.java`

- `extends AbstractLogicalOperator`; tag `LogicalOperatorTag.ORDER` (line 122).
- Order spec: `List<Pair<IOrder, Mutable<ILogicalExpression>>> orderExpressions`
  (line 97). `IOrder.OrderKind = {FUNCTIONCALL, ASC, DESC}` (lines 39–49);
  singletons `ASC_ORDER`/`DESC_ORDER`.
- **`topK` field (line 99):** `-1` = no limit push-down; `≥0` = a `LIMIT` was pushed
  into the sort → top-K path. *This single field decides `ExternalSort` vs `TopKSorter`.*
- `getVariablePropagationPolicy() == ALL` (line 136): sort preserves all columns.

### Stage 3 — Assign the physical operator

**File:** `hyracks-fullstack/algebricks/algebricks-rewriter/.../rules/SetAlgebricksPhysicalOperatorsRule.java`

`visitOrderOperator(OrderOperator oo, ...)` at **line 314**:
- **line 317:** top-level → `new StableSortPOperator(oo.getTopK())`
- **line 319:** nested (inside subplan / micro pipeline) → `new MicroStableSortPOperator()`

### Stage 4 — The physical operators

**`AbstractStableSortPOperator`** (`.../physical/AbstractStableSortPOperator.java`) — shared base:
- `MIN_FRAME_LIMIT_FOR_SORT = 3` (line 66) — hard floor.
- `computeLocalProperties()` (line 120): builds `OrderColumn[]` + `LocalOrderProperty`
  (each order expr must resolve to a `VARIABLE`, else `IllegalStateException`).
- `getRequiredPropertiesForChildren()` (line 88): for `PARTITIONED` execution, if
  parallel sort applies **and** a static `RangeMap` annotation (`USE_STATIC_RANGE`,
  line 102) is present → require `OrderedPartitionedProperty` (global range
  partitioning, line 106); else require `UNPARTITIONED` (merge at one site, line 109).
  Local requirement is always "each partition locally ordered."
- **`setupSortOperator()` (lines 157–188)** — the shared runtime-setup helper:
  builds the `RecordDescriptor` (line 161), `int[] sortFields` via
  `opSchema.findVariable(var)` (line 174), one `IBinaryComparatorFactory` per column
  (line 183), **one** `INormalizedKeyComputerFactory` for the *first* column only
  (lines 178–180), and reads **line 186:**
  `int maxNumberOfFrames = memReq.getMemoryBudgetInFrames();` → becomes `framesLimit`.

**`StableSortPOperator`** (`.../physical/StableSortPOperator.java`) — partitioned/global sort:
- Tag `STABLE_SORT`; `isMicroOperator() == false`.
- **`contributeRuntimeOperator()` (lines 61–82)** — the instantiation site:
  - line 65: `SortSetupData d = setupSortOperator(context, op, opSchema, sortColumns, localMemoryRequirements);`
  - **lines 69–71 (topK == -1):**
    `new ExternalSortOperatorDescriptor(spec, d.maxNumberOfFrames, d.sortFields, d.nkcf, d.comps, d.recDescriptor)`
  - **lines 72–77 (topK != -1):**
    `new TopKSorterOperatorDescriptor(spec, d.maxNumberOfFrames, topK, d.sortFields, d.nkcf, d.comps, d.recDescriptor)`

**`MicroStableSortPOperator`** (`.../physical/MicroStableSortPOperator.java`) — embedded sort:
- Tag `MICRO_STABLE_SORT`; `isMicroOperator() == true`.
- `contributeRuntimeOperator()` (lines 46–56): same `setupSortOperator`, then
  `new MicroSortRuntimeFactory(sortFields, nkcf, comps, null /*no projection*/, maxNumberOfFrames)`
  contributed via `builder.contributeMicroOperator(...)`. (See §13 — despite the name
  it *can* spill.)

**`SortMergeExchangePOperator`** (`.../physical/SortMergeExchangePOperator.java`) — the distributed merge connector:
- Not a compute operator — an **exchange/connector** (`extends AbstractExchangePOperator`),
  tag `SORT_MERGE_EXCHANGE`. Holds `OrderColumn[] sortColumns`.
- Requires each child input already locally ordered on `sortColumns`; delivers
  `UNPARTITIONED` + matching local order.
- **`createConnectorDescriptor()` (lines 130–156):** builds comparators + hash
  functions + first-column `nkcf`, then line 154
  `new MToNPartitioningMergingConnectorDescriptor(spec, tpcf, sortFields, comps, nkcf)`.
  This is the runtime piece that N-way-merges already-sorted partition streams — the
  "merge" half of a *distributed* sort (each partition runs `ExternalSort` locally,
  this connector merges them at one site).

### Stage 5 — The runtime descriptor

`StableSortPOperator` hands `maxNumberOfFrames` (=1024) to
`ExternalSortOperatorDescriptor` as its `framesLimit` (see §5 for the descriptor
internals). `framesLimit` then bounds both run generation and merge fan-in.

### Stage 6 — Where the memory budget / frame count actually comes from

Two sub-steps: **config → frame limit**, then a **rule stamps it onto the operator**.

**6a. Default bytes and the bytes→frames division**
- `CompilerProperties.java`: `COMPILER_SORTMEMORY` = **32 MB** (lines 46–49),
  `COMPILER_MIN_SORTMEMORY` = **512 KB** (lines 74–77), `COMPILER_FRAMESIZE` =
  **32 KB** (lines 70–73). `getSortMemoryFrames()` (line 412) = `32MB / 32KB = 1024`.
- `OptimizationConfUtil.getFrameLimit()` (lines 209–225): line 218
  `frameLimit = (int)(memBudget / frameSize)`, line 224 `return Math.max(frameLimit,
  minFrameLimit)` (min = 3). Query-level `SET \`compiler.sortmemory\`` overrides win
  because the query param is checked before the cluster default.
- `createPhysicalOptimizationConf()` line 114: `physOptConf.setMaxFramesExternalSort(sortFrameLimit)`.
- `PhysicalOptimizationConfig.java`: `getMaxFramesExternalSort()` (line 143) defaults
  to `32MB/frameSize`; `getMinSortFrames()` (line 198) defaults to 3.

**6b. The rule that writes the budget into the operator**
- `SetMemoryRequirementsRule.java`: calls `physOp.createLocalMemoryRequirements(op)`
  (variable budget, min 3) then visits with `MemoryRequirementsConfigurator`.
- `MemoryRequirementsConfigurator.visitOrderOperator()` (lines 166–170):
  `setOperatorMemoryBudget(op, physConfig.getMaxFramesExternalSort())` →
  `LocalMemoryRequirements.setMemoryBudgetInFrames(1024)` (validated against the
  min, else `ILLEGAL_MEMORY_BUDGET`).
- `LocalMemoryRequirements.VariableMemoryBudget`: `getMemoryBudgetInBytes(frameSize)
  = frameSize * frames`. This is exactly the value read back at
  `AbstractStableSortPOperator` line 186.

**Net:** default 32 MB / 32 KB = **1024 frames** → `new ExternalSortOperatorDescriptor(spec, 1024, ...)`.

### Stage 7 — Optimizer rules that reshape the sort

| Rule | File | What it does |
| :-- | :-- | :-- |
| **PushSortDownRule** | `algebricks-rewriter/.../PushSortDownRule.java` | `rewritePre` (line 46): if `ORDER` sits above an `ASSIGN` and the sort keys aren't produced by that assign, swap them (lines 81–83) to move the sort nearer where its keys originate. Only through `ASSIGN`, one step at a time. |
| **PushGroupByIntoSortRule** | `algebricks-rewriter/.../PushGroupByIntoSortRule.java` | `rewritePost` (line 55): a `PRE_CLUSTERED_GROUP_BY` above a `STABLE_SORT` with an `AGGREGATE`-rooted nested plan is fused: line 106 `op.setPhysicalOperator(new SortGroupByPOperator(...))` — one operator sorts *and* aggregates. |
| **SortGroupByPOperator** | `.../physical/SortGroupByPOperator.java` | `contributeRuntimeOperator` (line 119): line 240 `framesLimit = localMemoryRequirements.getMemoryBudgetInFrames()`, line 241 `new SortGroupByOperatorDescriptor(spec, framesLimit, keys, ...)`. |
| **PushLimitIntoOrderByRule** | (asterix rules) | Sets `OrderOperator.topK` so `StableSortPOperator` emits a `TopKSorterOperatorDescriptor`. |
| **RemoveSortInFeedIngestionRule** | `asterixdb/asterix-algebra/.../optimizer/rules/RemoveSortInFeedIngestionRule.java` | `rewritePost` (line 39): for `INSERT_DELETE_UPSERT` when blocking operators are disabled (line 47), if the input is an `ORDER` (line 52), splice the sort out (line 53) — feed ingestion must be non-blocking. |

### One-line summary of the path

```
SQL++ ORDER BY
  → OrderOperator (logical, tag ORDER, topK field)
  → [rules: PushSortDown, PushLimitIntoOrderBy, PushGroupByIntoSort, RemoveSortInFeed]
  → SetAlgebricksPhysicalOperatorsRule → StableSortPOperator | MicroStableSortPOperator
  → SetMemoryRequirementsRule stamps 1024 frames (32MB/32KB)
  → StableSortPOperator.contributeRuntimeOperator
  → new ExternalSortOperatorDescriptor(spec, 1024, sortFields, nkcf, comps, recDesc)
        (or TopKSorterOperatorDescriptor if topK != -1;
         or MicroSortRuntimeFactory for the micro operator)
  → [distributed] SortMergeExchangePOperator → MToNPartitioningMergingConnectorDescriptor
```

---

## 3. Memory budget: how bytes become "frames"

Everything in Hyracks is measured in **frames** — fixed-size byte buffers. The
sort operator's memory budget is expressed as a *number of frames*, computed from
a byte budget and the frame size.

**The defaults** (`CompilerProperties.java:46–72`):

```java
COMPILER_SORTMEMORY( LONG_BYTE_UNIT,
    StorageUtil.getLongSizeInBytes(32L, MEGABYTE),   // 32 MB per sort, per partition
    "The memory budget (in bytes) for a sort operator instance in a partition"),
...
COMPILER_FRAMESIZE( ...,
    StorageUtil.getIntSizeInBytes(32, KILOBYTE),      // 32 KB frames
    ...),
```

**The bytes→frames conversion** (`OptimizationConfUtil.java:208–225`):

```java
int frameLimit = (int) (memBudget / frameSize);          // 32MB / 32KB = 1024
if (frameLimit < minFrameLimit) { throw ...INVALID_FRAME_BASED_MEMORY_BUDGET }
return Math.max(frameLimit, minFrameLimit);              // minFrameLimit = 3
```

So with stock settings a single sort operator instance gets **`framesLimit = 1024`
frames**. `getSortNumFrames()` (lines 186–191) is the entry point;
`CompilerProperties.getSortMemoryFrames()` (lines 412–413) does the same division
`(int) getSortMemorySize() / getFrameSize()`.

This `framesLimit` (the *total*) is the single most important number in the whole
operator. Note two immediate deductions the code makes from it:

- **Run generation** gets `maxSortFrames = framesLimit - 1`
  (`AbstractExternalSortRunGenerator.java:62`) — one frame is reserved for output.
- **Merge** gets `maxMergeWidth = framesLimit - 1`
  (`AbstractExternalSortRunMerger.java:75`) — one frame reserved for the output
  frame; the rest are input frames, one per run being merged. This is the fan-in.

Per-query override: `SET \`compiler.sortmemory\` "128MB";`. The value is read from
`querySpecificConfig` first, falling back to the cluster default
(`OptimizationConfUtil.java:188–190`).

---

## 4. The frame byte format (physical tuple layout)

Before we can understand the sort, we must understand what a **frame** looks like
byte-for-byte, because the sorter reads/writes these offsets directly.

The canonical diagram (from `AbstractFrameAppender.java:33–44`):

```
Frame (a ByteBuffer of size = frameCount * initialFrameSize)
 _______________________________________________________________
| [tuple0][tuple1][tuple2] .....                                 |
|                        .                                       |
| ..[tupleN-1]      <free space>   [tupleOffsets (4*N)][count(4)]|
|_______________________________________________________________|
   ^grows right ->                          <- grows left^
```

### Frame-level metadata (`FrameConstants.java`)

| Constant | Value | Meaning |
| :-- | :-- | :-- |
| `META_DATA_FRAME_COUNT_OFFSET` | `0` | First int of the frame = **frameCount** (how many `initialFrameSize` units this frame spans). Actual size = `frameCount * initialFrameSize`. Lets a "frame" be a multiple of 32 KB for oversized tuples. |
| `TUPLE_START_OFFSET` | `5` | Tuple data begins at byte 5 (first int is frameCount; byte 4 reserved). |
| `SIZE_LEN` | `4` | Size of the tuple-count int stored at the very end. |
| `getTupleCountOffset(frameSize)` | `frameSize - 4` | Last 4 bytes hold **N = tupleCount** (`FrameHelper.java:26`). |

### The tuple-offset directory (grows backwards from the end)

- Tuple count `N` sits at `frameSize - 4`.
- The **end offset of tuple `i`** sits at `frameSize - 4*(i+1) - 4`
  → in code: `getTupleCountOffset - SIZE_LEN*(tupleIndex+1)`
  (`FrameTupleAccessor.getTupleEndOffset`, lines 83–86).
- Tuple `i`'s **start** = tuple `i-1`'s end; tuple 0 starts at `TUPLE_START_OFFSET (5)`
  (`getTupleStartOffset`, lines 71–75).

### Inside one tuple (the field-slot layout)

Each tuple is: **field-end-offset slots** followed by **field data**.

```
[ f0end | f1end | ... | f{F-1}end ][ field0 bytes | field1 bytes | ... ]
  <---- fieldSlotsLength = F*4 ---->  <----------- actual data --------->
```

- `getFieldSlotsLength() = fieldCount * 4` (`FrameTupleAccessor.java:111–113`).
- Field `f`'s end offset (relative to end of the slot region) is the int at
  `tupleStart + f*4` (`getFieldEndOffset`, lines 96–98).
- Field `f`'s start = field `f-1`'s end; field 0's relative start = 0
  (`getFieldStartOffset`, lines 89–93).
- So the absolute byte position of field `f` =
  `tupleStart + fieldSlotsLength + fieldStartOffset` (`getAbsoluteFieldStartOffset`, 78–80).

This is why the comparators receive `(byte[] buffer, int start, int length)`:
they operate directly on the shared frame array with computed offsets — **no
deserialization, no object allocation**.

### Writing tuples (`FrameTupleAppender` / `AbstractFrameAppender`)

- `hasEnoughSpace(fieldCount, tupleLength)` (`AbstractFrameAppender.java:62–65`):
  checks `tupleDataEndOffset + requiredSpace + tupleCount*4 <= getTupleCountOffset`.
  The `+ tupleCount*4` accounts for the growing offset directory at the tail —
  the two ends must not collide.
- `append(accessor, tStart, tEnd)` (`FrameTupleAppender.java:130–145`) is the hot
  path used by the sorter's flush: it `System.arraycopy`s the whole tuple, writes
  its new end-offset into the tail directory, and bumps the count. Verbatim byte
  copy — field slots are already inside `[tStart,tEnd)`.
- `canHoldNewTuple` (`AbstractFrameAppender.java:113–123`): if a *single* tuple is
  bigger than the frame and the frame is empty, it **grows the frame**
  (`ensureFrameSize` → `calcAlignedFrameSizeToStore`, rounding up to a multiple of
  the min frame size). This is the "big tuple" escape hatch.

`VSizeFrame` (`hyracks-api/.../comm/VSizeFrame.java`) is the resizable frame:
`ensureFrameSize` grows, `resize` sets exact size, `reset` shrinks back to
`minFrameSize` and clears.

---

## 5. The operator descriptor & its two activities

**File:** `AbstractSorterOperatorDescriptor.java` (the shared skeleton) and
`ExternalSortOperatorDescriptor.java` (the concrete external sort).

### Construction (`ExternalSortOperatorDescriptor.java`)

The fully-specified constructor (lines 110–120):

```java
ExternalSortOperatorDescriptor(spec, framesLimit, sortFields,
    keyNormalizerFactories, comparatorFactories, recordDescriptor,
    alg /*MERGE_SORT*/, policy /*LAST_FIT*/, outputLimit /*MAX_VALUE for full sort*/)
```

- `if (framesLimit <= 1) throw` — **minimum 2 frames** (1 in, 1 out) at the
  descriptor level (line 114–116). (The physical operator promises ≥3.)
- `alg` defaults to `Algorithm.MERGE_SORT` (line 40).
- `policy` defaults to `EnumFreeSlotPolicy.LAST_FIT` (line 41).
- `outputLimit` = `Integer.MAX_VALUE` for a normal sort; a finite value turns this
  into a top-K sort (only the first K tuples are emitted).

The abstract base stores `sortFields`, `keyNormalizerFactories`,
`comparatorFactories`, `framesLimit`, and the output `RecordDescriptor`
(`AbstractSorterOperatorDescriptor.java:55–69`). Note `super(spec, 1, 1)` — the
operator has **1 input, 1 output**.

### Two activities joined by a blocking edge (`contributeActivities`, lines 75–87)

```java
SortActivity  sa = getSortActivity(new ActivityId(odId, SORT_ACTIVITY_ID=0));
MergeActivity ma = getMergeActivity(new ActivityId(odId, MERGE_ACTIVITY_ID=1));
builder.addActivity(this, sa);
builder.addSourceEdge(0, sa, 0);     // operator input 0 -> sort activity
builder.addActivity(this, ma);
builder.addTargetEdge(0, ma, 0);     // merge activity -> operator output 0
builder.addBlockingEdge(sa, ma);     // MERGE cannot start until SORT finishes
```

The **blocking edge** is the reason sort is a *pipeline breaker* /
"expensiveThanMaterialization" (`AbstractStableSortPOperator.java:206–209`): all
input must be consumed and turned into runs before any output is produced.

### SortActivity — a *sink* that builds runs (lines 98–149)

`createPushRuntime` returns an `AbstractUnaryInputSinkOperatorNodePushable`:

- `open()`  → `runGen = getRunGenerator(...); runGen.open();`
- `nextFrame(buffer)` → `runGen.nextFrame(buffer);`  (every incoming frame)
- `close()` → finalizes: creates a `SortTaskState`, calls `runGen.close()`,
  stashes **the generated runs** and **the in-memory sorter** into the state
  object, and publishes it via `ctx.setStateObject(state)` (lines 126–135).

The `SortTaskState` (lines 89–96) is the hand-off vehicle between the two
activities: it carries `List<GeneratedRunFileReader> generatedRunFileReaders`
and `ISorter sorter`.

For `ExternalSortOperatorDescriptor`, `getRunGenerator` builds an
`ExternalSortRunGenerator` (lines 78–85).

### MergeActivity — a *source* that emits sorted output (lines 151–219)

`createPushRuntime` returns an `AbstractUnaryOutputSourceOperatorNodePushable`
whose `initialize()`:

1. Retrieves the `SortTaskState` produced by the Sort activity of the *same
   partition* (lines 170–173).
2. Rebuilds `IBinaryComparator[]` and the single `INormalizedKeyComputer`
   (lines 174–179).
3. Builds the merger via `getSortRunMerger(...)` with `framesLimit`
   (lines 180–181).
4. **Two cases** (lines 183–210):
   - **No runs were spilled** (`runs.isEmpty()`): everything is still in memory.
     `prepareSkipMergingFinalResultWriter(writer)` (a pass-through), then
     `sorter.flush(wrappingWriter)` streams the already-sorted in-memory tuples
     straight out. *No merge, no disk.* This is the pure in-memory fast path.
   - **Runs exist:** eagerly `sorter.close()` to free memory (line 192), then
     `merger.process(wrappingWriter)` runs the multi-pass merge (§11).

For `ExternalSortOperatorDescriptor`, `getSortRunMerger` builds an
`ExternalSortRunMerger` (lines 88–101).

---

## 6. Phase 1: run generation

### The interface & skeleton

`IRunGenerator` extends `IFrameWriter` (open/nextFrame/close/fail) and adds
`getRuns()` + `getSorter()`.

`AbstractSortRunGenerator.java` holds the `List<GeneratedRunFileReader>` and the
core flush logic:

- `close()` (lines 49–59): if the sorter still has tuples:
  - **no runs yet** → just `sorter.sort()` (keep in memory, the fast path above
    will flush it directly). *No spill.*
  - **runs already exist** → `flushFramesToRun()` (spill the last batch too, so
    everything is uniformly on disk for the merge).
- `flushFramesToRun()` (lines 66–82) — the spill routine:
  1. `sorter.sort()` — sort the in-memory pointers.
  2. `getRunFileWriter()` — create a fresh temp run file.
  3. `sorter.flush(flushWriter)` — write sorted tuples out (§7 flush).
  4. `generatedRunFileReaders.add(runWriter.createDeleteOnCloseReader())`.
  5. `sorter.reset()` — wipe memory, ready for the next run.

### Building the machinery (`AbstractExternalSortRunGenerator.java:56–74`)

```java
maxSortFrames = framesLimit - 1;                          // reserve 1 for output
IFrameFreeSlotPolicy freeSlotPolicy =
    FrameFreeSlotPolicyFactory.createFreeSlotPolicy(policy, maxSortFrames);
IFrameBufferManager bufferManager = new VariableFrameMemoryManager(
    new VariableFramePool(ctx, maxSortFrames * ctx.getInitialFrameSize()),
    freeSlotPolicy);
frameSorter = (alg == MERGE_SORT)
    ? new FrameSorterMergeSort(ctx, bufferManager, maxSortFrames, sortFields,
          keyNormalizerFactories, comparatorFactories, recordDesc, outputLimit)
    : new FrameSorterQuickSort(...);
```

So the sorter's memory budget in **bytes** is `maxSortFrames * initialFrameSize`
= `(1024-1) * 32KB ≈ 32 MB`.

### The core loop (`nextFrame`, lines 76–84)

```java
public void nextFrame(ByteBuffer buffer) {
    if (!frameSorter.insertFrame(buffer)) {   // returns false = out of memory
        flushFramesToRun();                   // sort+spill current contents
        if (!frameSorter.insertFrame(buffer)) // retry into now-empty sorter
            throw "frame too big to insert into the sorting memory";
    }
}
```

This is the beating heart of Phase 1: **fill until full, sort, spill, repeat.**
Each spill produces one sorted run. `insertFrame` returning `false` is the
"memory is full" signal (§7).

---

## 7. The frame sorter: tuple pointers & normalized keys

**File:** `AbstractFrameSorter.java` — the single most important file for
understanding *what bytes go where*.

### The pointer array `tPointers` (the thing that actually gets sorted)

Each in-memory tuple is described by a small record of `ptrSize` ints inside one
flat `int[] tPointers`. The layout per tuple (constants at lines 50–53):

```
tPointers[ ptr*ptrSize + 0 ] = ID_FRAME_ID       -> which logical frame holds it
tPointers[ ptr*ptrSize + 1 ] = ID_TUPLE_START    -> tuple start byte offset
tPointers[ ptr*ptrSize + 2 ] = ID_TUPLE_END      -> tuple end byte offset
tPointers[ ptr*ptrSize + 3 .. ] = ID_NORMALIZED_KEY  -> normalized key ints
```

`ptrSize = ID_NORMALIZED_KEY(3) + normalizedKeyTotalLength` (line 120). So with a
4-int normalized key, each tuple costs `(3+4)*4 = 28 bytes` of pointer overhead —
tiny compared to the tuple itself, and crucially it's what gets shuffled instead
of the data.

### Normalized keys (the comparison accelerator)

A *normalized key* maps a typed value onto one or more ints such that comparing
the ints in unsigned order gives the same answer as the real comparator (for the
common cases). Setup (lines 96–119):

- Only the **first** sort column normally has a normalizer (from
  `setupSortOperator`), but the code supports several.
- `getDecisivePrefixLength()` (`NormalizedKeyUtils.java:40–50`): how many leading
  normalizers are **decisive** (a difference in the normalized key is a definitive
  order, no tie-break needed). It takes the decisive prefix plus at most one
  indecisive normalizer (lines 99–103).
- `normalizedKeysDecisive` (line 113) = the normalized keys alone fully determine
  order for *all* sort columns → then we can skip byte comparison entirely.

### Inserting a frame (`insertFrame`, lines 140–158)

```java
inputTupleAccessor.reset(inputBuffer);
if (tupleCount <= 0) return true;                       // empty frame, no-op
long requiredMemory = getRequiredMemory(inputTupleAccessor);
if (totalMemoryUsed + requiredMemory <= maxSortMemory
        && bufferManager.insertFrame(inputBuffer) >= 0) {
    totalMemoryUsed += requiredMemory;
    tupleCount += inputTupleAccessor.getTupleCount();
    return true;                                         // accepted
}
if (getFrameCount() == 0)                                // even 1 frame won't fit
    throw FRAME_BIGGER_THAN_SORT_MEMORY;
return false;                                            // memory full -> caller spills
```

`getRequiredMemory` (lines 160–162) =
`frameCapacity + ptrSize * tupleCount * 4` — i.e. the raw frame bytes **plus** the
pointer-array bytes those tuples will need. The **merge-sort** variant adds
another `ptrSize*tupleCount*4` (`FrameSorterMergeSort.java:57–60`) because it
needs a **scratch copy** `tPointersTemp` of equal size. This is why merge sort
effectively "reserves" double pointer memory up front.

### Building the pointers (`sort`, lines 164–200)

`sort()` first materializes `tPointers` by scanning every logical frame in the
buffer manager, and for every tuple `j`:

```java
tPointers[ptr*ptrSize + ID_FRAME_ID]    = i;       // frame index
tPointers[ptr*ptrSize + ID_TUPLE_START] = tStart;  // from accessor
tPointers[ptr*ptrSize + ID_TUPLE_END]   = tEnd;
// then, for each normalizer, compute the normalized key straight into tPointers:
nkcs[k].normalize(array, fieldStartOffset, fieldLen, tPointers, keyPos);
```

Note `fieldStartOffset = fieldStartOffsetRel + tStart + fieldSlotsLength`
(line 190) — exactly the absolute field position from §4. After the pointers +
keys are built, `sortTupleReferences()` (the abstract hook) runs the chosen
algorithm (§8).

### The comparator (`compare`, lines 241–283) — the two-tier compare

```java
if (nkcs != null) {
    int c = NormalizedKeyUtils.compareNormalizeKeys(tp1..NORM, tp2..NORM, normalizedKeyTotalLength);
    if (c != 0 || normalizedKeysDecisive) return c;    // fast path: int compare only
}
// slow path: resolve the real bytes and run the type comparators field-by-field
for each sort field f:
    locate (b1,s1,l1) and (b2,s2,l2) via the frame arrays + field slots
    int c = comparators[f].compare(b1, s1, l1, b2, s2, l2);
    if (c != 0) return c;
return 0;
```

Tier 1 is a pure unsigned-int loop (`NormalizedKeyUtils.java:29–38`). Tier 2 is
only reached when normalized keys tie **and** they aren't decisive — it re-reads
field offsets directly from the frame `byte[]` using `IntSerDeUtils.getInt` and
calls the actual `IBinaryComparator`. No allocation on either path.

### Flushing sorted tuples (`flush`, lines 214–239)

Walks `tPointers` in sorted order `ptr = 0 .. min(tupleCount, outputLimit)`, and
for each: look up its frame via `bufferManager.getFrame`, then
`FrameUtils.appendToWriter(writer, outputAppender, inputTupleAccessor, tStart, tEnd)`
copies the tuple bytes into the output frame; when the output frame fills it is
flushed to the `writer` and reused. `outputLimit` is what makes top-K cheap here.

---

## 8. The two in-memory sort algorithms

Both sort **only the `tPointers` array** (via `swap`/`copy` of `ptrSize`-int
slices); tuple bytes never move. Selected by the `Algorithm` enum
(`Algorithm.java`), default `MERGE_SORT`.

### `FrameSorterMergeSort` — stable, bottom-up merge sort

**File:** `FrameSorterMergeSort.java:68–126`.

```java
void sort(int offset, int length) {
    int step = 1;
    while (step < length) {                 // bottom-up: widths 1,2,4,8,...
        for (int i = offset; i < end; i += 2*step) {
            int next = i + step;
            if (next < end) merge(i, next, step, min(step, end-next));
            else            copy(tPointers, i, tPointersTemp, i, end-i);
        }
        step *= 2;
        swap tPointers <-> tPointersTemp;   // ping-pong buffers
    }
}
```

- `merge` (lines 93–118): standard two-run merge using `compare(pos1, pos2)`;
  `cmp <= 0` takes the left element first → **stable** (equal keys keep input
  order). Copies are `ptrSize`-int `System.arraycopy`s.
- Requires the scratch array `tPointersTemp` of the same size (hence the extra
  memory reservation in §7). **This is the default** because ORDER BY is expected
  to be stable.

### `FrameSorterQuickSort` — 3-way (Dutch-flag) quicksort, in place

**File:** `FrameSorterQuickSort.java:43–101`.

- Classic Bentley–McIlroy 3-way partition around the middle element `m`
  (lines 43–89): elements `== pivot` are swapped to the ends and then
  `vecswap`ped back to the middle, so duplicates aren't re-partitioned.
- `swap` (lines 97–101) exchanges two `ptrSize`-int pointer slices via the
  reusable `tmpPointer` scratch (allocated once, `AbstractFrameSorter.java:130`).
- In place → no `tPointersTemp`, so it uses **less pointer memory** than merge
  sort, but it is **not stable**.

---

## 9. The memory manager (buffer pool)

Where the raw frame bytes actually live during Phase 1. Two layers:

### `VariableFramePool` — the byte allocator (`VariableFramePool.java`)

Owns the byte budget (`memBudget = maxSortFrames * initialFrameSize`, line 66 in
the run generator). Key ideas:

- Keeps an `ArrayList<ByteBuffer> buffers` of allocated frames and a `BitSet used`
  (lines 39–41).
- `allocateFrame(frameSize)` (lines 76–86) tries, in order:
  1. `findExistingFrame` — **binary-search** the free buffers (kept sorted by
     capacity) for a reusable one ≥ the requested size (lines 100–126).
  2. `createNewFrame` — if there's budget left, allocate a fresh buffer
     (lines 134–138).
  3. `mergeExistingFrames` — if fragmented, **coalesce** several free buffers'
     capacities into one big enough buffer (lines 149–161). Returns `null` if it
     still can't satisfy → that's the ultimate "out of memory" signal.
- `reset()` (lines 172–177): drop nulls, re-sort buffers by size, clear `used` —
  reuse all buffers for the next run without re-allocating from the OS.

### `VariableFrameMemoryManager` — logical vs physical frames (`VariableFrameMemoryManager.java`)

This is what the sorter actually calls (`bufferManager`). It packs *logical*
input frames into *physical* pool buffers, so a big physical buffer can hold
several small logical frames back-to-back.

- `insertFrame(frame)` (lines 100–122):
  1. `findAvailableFrame(frameSize)` — ask the free-slot policy for a physical
     buffer that already has ≥ `frameSize` free tail space; else allocate a new
     one from the pool (lines 59–76).
  2. `System.arraycopy(frame.array(), 0, buffer.array(), offset, frameSize)` —
     copy the incoming frame's bytes into the physical buffer at `offset`
     (line 110).
  3. If tail space remains, register it with the free-slot policy
     (`pushNewFrame`, lines 111–113).
  4. Record a `BufferInfo(buffer, offset, frameSize)` as logical frame N; return N
     (lines 115–121).
- `getFrame(i, info)` (lines 86–93): hand back the `BufferInfo` for logical frame
  `i` (buffer + start offset + length). This is what `AbstractFrameSorter` uses to
  resolve `ID_FRAME_ID` back to real bytes.

### `FrameFreeSlotLastFit` — the placement policy (`FrameFreeSlotLastFit.java`)

Default `EnumFreeSlotPolicy.LAST_FIT`. Maintains an array of `(frameId, freeSpace)`
records. `popBestFit(size)` (lines 56–66) scans **from the most-recently-pushed
end backwards** for the first slot with enough free space, removes it, returns its
frame id. "Last fit" = prefer recently-touched buffers (better cache/locality),
O(n) worst case but usually short. Alternatives exist (`BIGGEST_FIT` = a sorted
tree) via `FrameFreeSlotPolicyFactory`.

---

## 10. Spilling a run to disk

When `flushFramesToRun` fires (§6), the sorted tuples are written through a
`RunFileWriter` (`RunFileWriter.java`):

- `getRunFileWriter()` (`ExternalSortRunGenerator.java:56–61`) creates a
  **managed workspace temp file** via
  `ctx.getJobletContext().createManagedWorkspaceFile("ExternalSortRunGenerator")`
  and wraps it in a `RunFileWriter` over the node's `IIOManager`.
- `nextFrame(buffer)` (lines 63–68) does a `ioManager.syncWrite(handle, size,
  buffer)` and tracks `maxOutputFrameSize` = the largest frame written (needed
  later so the merge can size its read buffers for oversized tuples).
- `createDeleteOnCloseReader()` (lines 107–112) returns a `GeneratedRunFileReader`
  that remembers `size` and `maxFrameSize` and **auto-deletes the temp file when
  the reader is closed** — that's how spilled runs get cleaned up after the merge.

So a "run" on disk is simply a sequence of the same frame byte-format from §4,
already in sorted order, in a temp file.

---

## 11. Phase 2: the multi-pass merge

**File:** `AbstractExternalSortRunMerger.java` (driver) + `ExternalSortRunMerger.java`
(concrete file/writer hooks).

The merger's fan-in budget is `maxMergeWidth = framesLimit - 1` (line 75) — one
output frame reserved, the rest are one read frame per run.

### The driver loop (`process`, lines 80–143)

```
currentGenerationRunAvailable = all runs [0, stop)
while (true):
    unUsed = selectPartialRuns(maxMergeWidth * frameSize, runs, partialRuns, ...)
    prepareFrames(unUsed, inFrames, partialRuns)
    if (more runs remain OR we're not yet at the final group):
        if partialRuns.size()==1: reader = that run          # nothing to merge
        else:
            mergeFileWriter = new intermediate run file
            merge(mergeResultWriter, partialRuns)             # K-way merge -> disk
            reader = mergeFileWriter.createDeleteOnCloseReader()
        runs.add(reader)                                      # feed back for next pass
        if this generation is exhausted:
            numberOfPasses++
            runs.subList(0, stop).clear()                     # drop consumed runs
            reset availability over the newly-produced runs
            stop = runs.size()
    else:
        merge(finalWriter, partialRuns)                       # last group -> output
        break
```

This is a **generational, multi-pass merge**. Each pass consumes the current
generation of runs in groups of ≤ `maxMergeWidth`, producing a smaller next
generation, until one final group merges straight to the operator's output.
Number of passes ≈ `ceil(log_maxMergeWidth(numInitialRuns))`.

### Choosing which runs go in a group (`selectPartialRuns`, lines 145–162)

Greedy by **byte budget**, not by count: starting from the first available run,
add runs while their `getMaxFrameSize()` fits in the remaining
`maxMergeWidth * frameSize` budget. Runs with oversized frames cost more budget,
so a group may hold fewer than `maxMergeWidth` runs. Returns leftover budget.

### Spending leftover budget on bigger read buffers (`prepareFrames`, lines 164–190)

If budget remains after selecting the group, it's **distributed as extra capacity
across the input frames** (each run's read buffer is grown by `avg`, residue
spread one-frame-at-a-time), capped at `MAX_FRAMESIZE`. Bigger read buffers = each
`nextFrame` from disk pulls more data = fewer I/O syscalls. Then it sizes each
`GroupVSizeFrame` read buffer to its run's max frame size (lines 180–189).

### The actual merge of a group (`merge`, lines 192–206)

```java
RunMergingFrameReader merger = new RunMergingFrameReader(ctx, partialRuns,
        inFrames, sortFields, comparators, nmkComputer, recordDesc, topK);
merger.open();
while (merger.nextFrame(outputFrame))
    FrameUtils.flushFrame(outputFrame.getBuffer(), writer);
```

`GroupVSizeFrame` (`util/GroupVSizeFrame.java`) + `GroupFrameAccessor`
(`util/GroupFrameAccessor.java`) let one physical read buffer hold **several
logical frames** read from disk at once and be parsed as if it were one frame
(`GroupFrameAccessor.parseGroupedBuffer`, lines 142–159, walks the concatenated
sub-frames using each frame's leading `frameCount`). This amortizes disk reads.

---

## 12. The K-way merge heap

**File:** `RunMergingFrameReader.java` + `util/ReferencedPriorityQueue.java` +
`util/ReferenceEntry.java`.

### Setup (`open`, `RunMergingFrameReader.java:81–102`)

- One `GroupFrameAccessor` per run, each reset to that run's first frame.
- A `ReferencedPriorityQueue topTuples` sized to the number of runs, with a
  comparator built from the type comparators + normalized key (lines 86–87).
- For each run: open it, read its first frame, seed the queue with its first tuple
  via `setNextTopTuple` (lines 89–101). Empty runs are popped immediately.

### Producing output (`nextFrame`, lines 104–126)

```java
while (!topTuples.areRunsExhausted() && tupleCount < topK):
    top = topTuples.peek();                       // current global-min tuple
    if (!outFrameAppender.append(top.fta, top.tupleIndex))
        return true;                              // output frame full -> emit it
    tupleCount++;
    tupleIndexes[runIndex]++;
    setNextTopTuple(runIndex, ...);               // pull the next tuple from that run
```

`setNextTopTuple` (lines 135–145): if the winning run has another tuple,
`topTuples.popAndReplace(...)` swaps the emitted min for that run's next tuple and
re-heapifies; if the run is exhausted, `pop()` it and close the run.
`hasNextTuple` (147–163) transparently advances to the run's next frame from disk
when the current frame is drained (recursively skipping empty frames).

### The priority queue itself (`ReferencedPriorityQueue.java`)

A fixed-size **tournament tree** ("loser tree" style) over the runs:

- `entries[]` is a binary-tree-shaped array; `entries[0]` is the current minimum
  (`peek`, lines 60–62).
- `add(e)` (lines 86–121) walks *up* from the leaf slot
  `slot = (size>>1) + (runid>>1)` toward the root, comparing against the entry at
  each level; the winner (smaller) bubbles toward `entries[0]`. It respects a
  `runAvail` `BitSet` so exhausted runs never win.
- `popAndReplace(fta, tIndex)` (lines 72–79): reuse `entries[0]`, point it at the
  new tuple, `add` it back — O(log K) sift with no allocation.
- `pop()` (lines 128–134): mark this run unavailable and sift, shrinking the live
  set; `areRunsExhausted()` is true when `runAvail` is empty.

### What a queue entry stores (`ReferenceEntry.java`)

Crucially, an entry does **not** copy the tuple. It stores:

- `runid`, the current `IFrameTupleAccessor`, the `tupleIndex`, and
- `int[] tPointers` = `[normalizedKey ints...] [fieldStart, fieldLen] per key field]`
  (lines 29, 39). `initTPointer` (69–82) fills the per-key `(absoluteFieldStart,
  fieldLength)` and normalizes the first key field into the prefix.

So the merge comparator (`createEntryComparator`,
`RunMergingFrameReader.java:174–214`) does the same **two-tier** trick as the
in-memory sorter: compare normalized-key ints first (lines 180–186), and only on
a tie fall through to the real byte comparators (lines 192–204), finally
tie-breaking on `runid` to keep the merge **stable**.

---

## 13. Variants

The unifying theme: **tuple bytes are written once into buffer-manager frames;
ordering is done over compact side arrays of normalized keys + pointers; and a
per-path output limit (`outputLimit`/`topK`) or an aggregation writer
(`PreclusteredGroupWriter`) is layered onto the *same* flush/merge plumbing** to
produce top-K sort or sort-group-by without changing the core sort.

### 13a. Top-K / LIMIT sort

Used for `ORDER BY ... LIMIT k` (and `LIMIT k OFFSET n` → `topK = k+n`). Instead
of sorting all N tuples, keep only the k smallest seen so far. Chosen when the
optimizer pushed a limit into the sort (§2 stage 2, `OrderOperator.topK`).

- **`TopKSorterOperatorDescriptor.java`** — thin descriptor storing `topK` (line 38).
  `getSortActivity` → run generator = **`HybridTopKSortRunGenerator`** (lines 64–65);
  `getMergeActivity` → a plain **`ExternalSortRunMerger` but with the `topK`
  argument** (lines 80–81), so the merge phase is *also* top-K bounded.

- **`HybridTopKSortRunGenerator.java`** (extends `HeapSortRunGenerator`) — adaptive:
  starts with a heap; if the heap has to spill more than
  `SWITCH_TO_FRAME_SORTER_THRESHOLD = 2` times (lines 41, 87–96) — a sign the data
  is too large / high-cardinality for a k-heap to pay off — it **permanently
  switches to a `FrameSorterMergeSort`** (`BIGGEST_FIT` pool, `frameLimit-1` frames,
  lines 98–116) that still emits only `topK` tuples by passing `topK` as the frame
  sorter's **`outputLimit`** (line 105; see `AbstractFrameSorter.flush`, §7). Rationale:
  heap is O(N log k) with tiny memory when k≪N; once repeated spills dominate,
  ordinary run-based merge with a top-K-limited flush is cheaper.

- **`HeapSortRunGenerator.java`** (base) — pure heap top-K run generation.
  `open()` builds a `VariableFramePool` of `(frameLimit-1)` frames wrapped in a
  **`VariableDeletableTupleMemoryManager`** (deletable because top-K *evicts*
  tuples) and a **`TupleSorterHeapSort`** (lines 64–71). `nextFrame` inserts each
  tuple, flushing+retrying on overflow.

- **`TupleSorterHeapSort.java`** — the heap engine (byte-level):
  - A **`MaxHeap`** of capacity `topK` (line 169) — root is the *largest* of the k
    kept tuples, i.e. the eviction candidate.
  - Each `HeapEntry` (lines 54–105) stores only `int[] nmk` (normalized-key ints)
    + a `TuplePointer` `(frameIndex, tupleIndex)` (2 ints). **No tuple bytes in the
    heap** — bytes live in the deletable buffer manager.
  - `insertTuple` (lines 182–206) — the admission test: compute the incoming
    normalized key; if the heap is full and the new tuple `>=` current max, **reject
    immediately** (line 190, no byte copy). Otherwise copy bytes in; if full,
    delete the old max's bytes (line 202) and `heap.replaceMax` (line 203).
  - `compareTo` (lines 64–92): normalized-key int compare first; only on a tie (and
    non-decisive key) resolve pointers and run real `IBinaryComparator`s over field
    bytes.
  - `sort()` (lines 264–269): heap order ≠ sorted order, so it grabs the backing
    array (`heap.getEntries()`) and `Arrays.sort`s the k entries ascending.
  - `flush()` (lines 282–299): append each entry's tuple (via the `TuplePointer`).

- **`ITupleSorter.java`** — extends `ISorter` with tuple-granular
  `insertTuple(accessor, index)` + `getTupleCount()`. Tuple-at-a-time insertion is
  precisely what enables the reject-before-copy admission test. (Contrast
  `IFrameSorter.insertFrame`, whole-frame.)

- **Heap structures** (`structures/`): `MaxHeap` (binary max-heap,
  `peekMax`/`replaceMax`/`getMax`), `AbstractHeap` (array-backed, entries reused via
  value-reset, doubling growth, `getEntries()` `@Deprecated` because reading it can
  break heap invariants — which is exactly what `sort()/flush()` exploit),
  `MinMaxHeap`/`IMinMaxHeap` (general min-max heap; top-K uses only `MaxHeap`).

- **Top-K in the merge phase:** `AbstractExternalSortRunMerger` stores `topK`
  (line 76), passes it to `RunMergingFrameReader` (line 194); the merge loop is
  `while (!areRunsExhausted() && tupleCount < topK)` (line 107) — stops after k
  global tuples.

### 13b. Sort-based group-by (`group/sort/`)

External sort with **aggregation fused into both spill and merge**, so consecutive
equal-key tuples collapse as early as possible. The only structural change vs plain
sort: every output path is wrapped in a **`PreclusteredGroupWriter`** (aggregates
runs of equal, already-adjacent group keys).

- **`SortGroupByOperatorDescriptor.java`** — stores `groupFields` (a prefix subset
  of `sortFields`), a **partial** aggregator factory + a **merge** aggregator
  factory, and hardcodes `ALG = MERGE_SORT` (line 56, needs stability). Two
  aggregators because aggregation is two-staged: partial (raw → intermediate) and
  merge (intermediate → intermediate/final). `getSortActivity` →
  `ExternalSortGroupByRunGenerator`; `getMergeActivity` → `ExternalSortGroupByRunMerger`.

- **`ExternalSortGroupByRunGenerator.java`** (extends `AbstractExternalSortRunGenerator`
  — same in-memory frame-sort machinery). The one override that matters:
  **`getFlushableFrameWriter` (lines 87–97)** wraps the `RunFileWriter` in a
  `PreclusteredGroupWriter` with the group-by comparators + the *partial*
  aggregator. Since data is sorted before flush, equal keys are contiguous and get
  aggregated on the way to disk → each run already holds partially-aggregated groups.

- **`ExternalSortGroupByRunMerger.java`** (extends `AbstractExternalSortRunMerger`).
  Because stored tuples now have the aggregation-output layout, it **re-bases** sort/
  group field indexes to `0..n-1` (constructor, lines 68–86). It returns a
  `PreclusteredGroupWriter` from *every* merge hook — the no-spill fast path (partial
  or merge aggregator), each intermediate pass, and the final pass (merge
  aggregator). A `localSide` flag swaps partial/merge aggregators to support the
  two-phase local/global group-by pattern.

### 13c. In-memory sort and micro sort

- **`InMemorySortOperatorDescriptor.java`** — a *pure* in-memory sort, **no spill,
  no merge passes** (extends `AbstractOperatorDescriptor`, not the sorter base).
  Sort activity builds a `FrameSorterMergeSort` over a `VariableFramePool` with
  `UNLIMITED_MEMORY` (lines 122–129); if `insertFrame` fails it **throws** telling
  you to raise memory or use ExternalSort (lines 133–139). `close()` sorts and
  stashes the sorter; the "merge" activity just `flush`es the single sorted batch
  (name is a misnomer here). Used when the planner can prove the input fits.

- **`MicroSortRuntimeFactory.java`** (`algebricks-runtime/.../operators/sort/`) — a
  push-runtime embeddable inside a larger meta-operator (subplan / group-per-group),
  produced by `MicroStableSortPOperator`. **Despite the class name
  `InMemorySortPushRuntime`, it is a bounded external sort that CAN spill:** `open()`
  lazily builds an `ExternalSortRunGenerator` (MERGE_SORT, LAST_FIT, `framesLimit`,
  no top-K; lines 91–93); `close()` either flushes in memory (no runs) or runs the
  full multi-pass `ExternalSortRunMerger.process` (lines 114–124).
  `createOrResetRunsMerger` (lines 170–183) can `reset()` the merger for reuse across
  repeated invocations (one per group/subplan). Projection lists are unsupported
  (`NotImplementedException`, lines 60–63).

### 13d. Enums / interfaces (quick reference)

| Type | Role |
| :-- | :-- |
| `Algorithm` | `{QUICK_SORT, MERGE_SORT}` — picks `FrameSorterQuickSort` vs `FrameSorterMergeSort` inside a run generator. MERGE_SORT is stable and is the default / hardcoded for group-by + micro/in-memory. |
| `ISorter` | Base contract: `hasRemaining/reset/sort/close/flush`. `flush` returns max frame size written (variable-frame protocol). |
| `IFrameSorter` | `ISorter` + `getFrameCount()` + `insertFrame(ByteBuffer)` (whole-frame). Impls: `FrameSorterMergeSort`, `FrameSorterQuickSort`. |
| `ITupleSorter` | `ISorter` + `getTupleCount()` + `insertTuple(accessor, i)` (one tuple). Only impl: `TupleSorterHeapSort`. |
| `IRunGenerator` | `IFrameWriter` + `getRuns()` + `getSorter()` (may be null). Impls: `ExternalSortRunGenerator`, `ExternalSortGroupByRunGenerator`, `HeapSortRunGenerator`, `HybridTopKSortRunGenerator`. |

### 13e. The deletable buffer manager (top-K's memory model)

`buffermanager/VariableDeletableTupleMemoryManager.java` backs the heap sorter
because top-K evicts tuples. Insert costs `4 + tupleLength` bytes
(`calculatePhysicalSpace`, 4 bytes for the offset slot). Deletion marks a tuple's
end-offset negative and accumulates a `deleted_space` counter in the frame's last 4
bytes (`DeletableFrameTupleAppender`); fragmentation is reclaimed lazily by
`reOrganizeFrames()` only when an insert can't otherwise fit. The heap array itself
only ever holds `(normalized-key ints + 2-int TuplePointer)` per entry.

---

## 14. End-to-end trace of one tuple

Putting it together, following a single record `r` with sort key `k`:

1. **Arrives** in an input frame at `SortActivity.nextFrame` → `runGen.nextFrame`
   (`AbstractSorterOperatorDescriptor.java:120–123`).
2. **Stored, not copied per-tuple:** the whole frame's bytes are copied once into
   a pool buffer by `VariableFrameMemoryManager.insertFrame`
   (`System.arraycopy`, line 110). `r`'s bytes now live at some
   `(physicalBuffer, offset)`.
3. **Pointer built:** on `sort()`, `r` gets a `tPointers` slot recording its
   `frameId`, `tStart`, `tEnd`, and the normalized form of `k`
   (`AbstractFrameSorter.java:176–195`).
4. **Sorted by proxy:** merge/quick sort permutes `r`'s 7-ish-int pointer slot
   relative to others using `compare` — mostly integer compares of normalized `k`
   (§7/§8). `r`'s actual bytes never move.
5. **Either:**
   - *Fits in memory:* on merge phase, `sorter.flush` copies `r` out in sorted
     order (`AbstractFrameSorter.flush`, 214–239). Done — never touched disk.
   - *Spilled:* `flushFramesToRun` writes `r` (in sorted position within its run)
     to a temp file via `RunFileWriter.nextFrame` (`syncWrite`).
6. **Merged (if spilled):** during Phase 2, `r`'s run is opened, `r` becomes a
   `ReferenceEntry`, competes in the tournament queue, and when it's the global
   min it's `append`ed to the output frame (`RunMergingFrameReader.nextFrame`,
   113). Possibly across several passes if there were many runs.
7. **Emitted** to the downstream operator via the output frame.

---

## 15. Glossary

| Term | Meaning | Source |
| :-- | :-- | :-- |
| **Frame** | Fixed-size byte buffer (default 32 KB), the unit of data movement | `FrameConstants`, `VSizeFrame` |
| **initialFrameSize** | The base frame size (`ctx.getInitialFrameSize()`), = `compiler.framesize` (32 KB) | `CompilerProperties.java:70` |
| **framesLimit** | Total frame budget for the operator instance = `sortmemory/framesize` (≈1024) | `OptimizationConfUtil.java:218` |
| **maxSortFrames** | `framesLimit - 1` (run generation; 1 reserved for output) | `AbstractExternalSortRunGenerator.java:62` |
| **maxMergeWidth** | `framesLimit - 1` (merge fan-in; 1 reserved for output) | `AbstractExternalSortRunMerger.java:75` |
| **Run** | A sorted temp file produced when memory fills | `ExternalSortRunGenerator`, `RunFileWriter` |
| **tPointers** | `int[]` of fixed-size per-tuple records that gets sorted in place of the data | `AbstractFrameSorter.java:75` |
| **ptrSize** | Ints per tuple pointer = `3 + normalizedKeyTotalLength` | `AbstractFrameSorter.java:120` |
| **Normalized key** | Order-preserving int encoding of a key for fast compares | `NormalizedKeyUtils`, `nkcs` |
| **outputLimit / topK** | Cap on emitted tuples; turns sort into top-K | `ExternalSortOperatorDescriptor.java:112` |
| **SortTaskState** | State object handing runs+sorter from Sort to Merge activity | `AbstractSorterOperatorDescriptor.java:89` |
| **Blocking edge** | Makes Merge wait for all of Sort (pipeline breaker) | `contributeActivities`, line 86 |
| **Default sort memory** | **32 MB** per operator instance per partition | `CompilerProperties.java:46–49` |

---

*Every section is verified against the source at the cited line numbers (line
numbers reflect the current checkout on `master`; if a file has since changed,
search for the quoted method/field name rather than trusting the line number).*

---

## Appendix A — Suggested reading order for a walkthrough

When we sit down to trace the code together, this is the order that builds
understanding fastest (each step depends only on the ones before it):

1. **§4 frame byte format** → `FrameConstants`, `FrameHelper`, `FrameTupleAccessor`.
   Nothing else makes sense until you can read a frame by hand.
2. **§5 the descriptor + two activities** → `ExternalSortOperatorDescriptor`,
   `AbstractSorterOperatorDescriptor`. See the sort/merge split and blocking edge.
3. **§6 run generation** → `AbstractExternalSortRunGenerator.nextFrame` (the
   fill→sort→spill loop).
4. **§7 the frame sorter** → `AbstractFrameSorter` (tPointers, normalized keys,
   `compare`, `flush`). This is the intellectual core.
5. **§8 the algorithms** → `FrameSorterMergeSort.sort` then `FrameSorterQuickSort.sort`.
6. **§9 memory** → `VariableFramePool` + `VariableFrameMemoryManager`.
7. **§11–§12 the merge** → `AbstractExternalSortRunMerger.process` then
   `RunMergingFrameReader.nextFrame` + `ReferencedPriorityQueue`.
8. **§2 call path** and **§13 variants** last — once the runtime is concrete, the
   optimizer path and the variants are easy to slot in.

## Appendix B — Hooks worth knowing for "memory-adaptive sort" research

Places where the current design makes a *static* memory decision — i.e. natural
intervention points for an adaptive scheme:

| Decision (currently static) | Where | Note |
| :-- | :-- | :-- |
| Total budget = `framesLimit` frames, fixed at compile time | §2 stage 6; `MemoryRequirementsConfigurator.visitOrderOperator` | Chosen before any data is seen; never revised at runtime. |
| Reserve exactly 1 frame for output (`maxSortFrames = framesLimit-1`) | `AbstractExternalSortRunGenerator.java:62` | Fixed split between input buffers and output. |
| Spill trigger = "next frame doesn't fit" | `AbstractFrameSorter.insertFrame` / `getRequiredMemory` | Purely reactive; no look-ahead, no partial spill. |
| Merge fan-in = `framesLimit-1`, greedy by byte budget | `AbstractExternalSortRunMerger.selectPartialRuns` | Same budget as run generation; not re-tuned per pass. |
| Extra merge budget spread evenly across read buffers | `prepareFrames` | A fixed heuristic, not adaptive to run access patterns. |
| Merge-sort reserves 2× pointer memory up front | `FrameSorterMergeSort.getRequiredMemory` | Quicksort needs 1×; algorithm choice is compile-time. |
| Heap↔frame switch threshold = 2 spills (top-K only) | `HybridTopKSortRunGenerator.java:41` | The *one* place the engine already adapts at runtime — a useful precedent/model. |

The `HybridTopKSortRunGenerator` adaptive switch (last row) is the closest thing
the codebase already has to runtime memory adaptivity, and is a good template for
how an adaptive policy can be threaded through the existing `IRunGenerator` /
`IFrameSorter` abstractions without disturbing the byte-level machinery.
