/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.dataflow.std.sort;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAppender;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.utils.NormalizedKeyUtils;
import org.apache.hyracks.dataflow.std.buffermanager.BufferInfo;
import org.apache.hyracks.dataflow.std.buffermanager.IFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.VariableFramePool;
import org.apache.hyracks.util.IntSerDeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractFrameSorter implements IFrameSorter {

    protected Logger LOGGER = LogManager.getLogger();
    protected static final int ID_FRAME_ID = 0;
    protected static final int ID_TUPLE_START = 1;
    protected static final int ID_TUPLE_END = 2;
    protected static final int ID_NORMALIZED_KEY = 3;

    // the length of each normalized key (in terms of integers)
    protected final int[] normalizedKeyLength;
    // the total length of the normalized key (in term of integers)
    protected final int normalizedKeyTotalLength;
    // whether the normalized keys can be used to decide orders, even when normalized keys are the same
    protected final boolean normalizedKeysDecisive;

    protected final int ptrSize;

    protected final int[] sortFields;
    protected final IBinaryComparator[] comparators;
    protected final INormalizedKeyComputer[] nkcs;
    protected final IFrameBufferManager bufferManager;
    protected final FrameTupleAccessor inputTupleAccessor;
    protected final IFrameTupleAppender outputAppender;
    protected final IFrame outputFrame;
    protected final int outputLimit;

    // protected final long maxSortMemory;
    protected long maxSortMemory; // non-final: memory-adaptive sort can change it between runs
    protected long totalMemoryUsed;
    protected int[] tPointers;
    protected final int[] tmpPointer;
    protected int tupleCount;

    // ============================================================================================
    // [Phase instrumentation] Ground truth for "does the sort operator interleave sort work with
    // data arrival, or load everything and then sort?"
    //
    // A CPU sampler cannot answer this: during the load phase the operator thread is running
    // System.arraycopy, so it reads as ~100% busy either way. What distinguishes the two is WHEN
    // comparison work happens, which only the sorter itself knows.
    //
    // Enable with -Dhyracks.sort.phaseLog=true. OFF by default: the nanoTime calls are cheap but
    // must not perturb timing runs. Emits one line per run:
    //   adaptive-sort-phase: loadNs=.. sortNs=.. cascadeNs=.. mergeNs=.. sortEvents=..
    //                        firstSortAtNs=.. lastSortAtNs=.. spanNs=..
    // firstSortAt/lastSortAt are relative to the first frame of the run, so their spread over the
    // run's lifetime IS the interleaving measure: load-then-sort collapses both to the very end.
    private static final boolean PHASE_LOG = Boolean.getBoolean("hyracks.sort.phaseLog");
    private long phLoadNs;
    private long phSortNs;
    private long phMergeNs;
    private int phSortEvents;
    private long phRunStartNs;
    private long phFirstSortAtNs;
    private long phLastSortAtNs;
    // Critical-path decomposition. insertFrame is called from the operator's nextFrame, so the GAP
    // between one insertFrame returning and the next starting is time the operator was NOT in the
    // sorter -- overwhelmingly, blocking for the upstream to hand over the next frame.
    //   phInsertNs high + phGapNs low  -> the sorter is the bottleneck; sort work is on the critical
    //                                     path and delays consumption (upstream backs up).
    //   phInsertNs low  + phGapNs high -> upstream is the bottleneck and sort work rides for free.
    // This is what distinguishes "interleaving overlaps with arrival" from "interleaving merely
    // postpones consumption".
    private long phCascadeNs; // cascade merges run from sealBucket -- previously UNCOUNTED
    private long phInsertNs;
    private long phGapNs;
    private long phLastInsertEndNs;
    // Periodic time series for plotting memory use and cumulative sort work against time.
    private static final long PHASE_SAMPLE_NS = 100L * 1000 * 1000; // 100ms
    private long phLastSampleNs;
    private int phRunSeq;
    // OPERATION COUNTS. Counts are noise-free, unlike timings: a 24% run-to-run timing spread does
    // not blur them at all. They separate "does more work" from "does the same work more slowly":
    //   same compares + more moves  -> data movement is the cost (pointer traffic)
    //   same compares + same moves  -> the memory system is the cost (cache/TLB), not the algorithm
    // A balanced pairwise merge of k runs costs N*log2(k) moves; a k-way tournament merge costs the
    // same comparisons but only N moves. These counters are what tells the two apart.
    // [Auto type key] A runtime-type-detecting normalizer is only sound while the sort column is
    // homogeneous: AsterixDB orders different types by type tag but numeric types by promoted value,
    // and no fixed-width key reproduces both. Such a normalizer reports isKeyValid()==false the
    // moment it sees a second distinct tag. When that happens the keys computed so far are NOT a
    // valid ordering, so we must stop consulting them AND discard any ordering already derived from
    // them -- which for this sorter means re-sorting the whole run with the comparator alone, since
    // buckets are sorted during accumulation.
    private boolean nkUsable = true;
    // [runtime decisiveness] A compile-time-indecisive normalizer can still turn out to have
    // produced injective keys -- an auto-detecting one does exactly that whenever the column is
    // homogeneous and its values fit the key. When that holds, equal normalized keys already prove
    // the tuples are equal, so compare() may return 0 without dereferencing tuple bytes. This
    // matters because the duplicate rate WITHIN a run grows with the run size: a large budget puts
    // many equal keys in the same run, and each one would otherwise cost a random-access comparator
    // call. Recomputed at every sort entry point, never cached across inserts.
    private boolean runtimeDecisive;
    private long phMoves; // tuple-slot moves (each is ptrSize ints)
    private long phCompares; // calls to compare()
    // Coverage check: span must equal gap + insert + sortCall + residual. A large residual means a
    // stage is UNINSTRUMENTED -- which is exactly how the cascade merge hid for so long.
    private long phSortCallNs;
    private long phFlushNs;
    // [ADDED] Incremental cache-sized bucket sort. builds each frame's pointers as they arrive.
    // [Experiment harness] Cache-sized bucket target. Override without a rebuild via
    // -Dhyracks.sort.bucketTargetBytes=N on the NC JVM. Two notable settings:
    //   262144 (default)  ~L2; the incremental bucket sort as designed
    //   a huge value      no bucket ever seals during accumulation, so sort() does ONE big sort at
    //                     flush -- i.e. Stage 1 disabled, the stock-sorter baseline in-build.
    private static final long BUCKET_TARGET_BYTES = Long.getLong("hyracks.sort.bucketTargetBytes", 256L * 1024);
    // [measured 2026-09-01] A CONSTANT bucket target makes the bucket COUNT grow with the budget,
    // and the cascade then costs more per tuple: +12-18% at 320MB-2GB. The fix is to bound the
    // count, not to abandon bucketing -- the sweep showed coarse buckets (>=64k tuples) run at
    // 4.22-4.31s versus 4.28s with bucketing off, i.e. free. Keeping buckets matters because
    // sealing is what makes the rest of the design work: getSortedTupleCount() only advances on a
    // seal, so with no buckets Stage 2's cheap shrink (spill the sorted prefix, keep the unsorted
    // tail) degenerates to a stock full flush -- precisely at the large budgets where a broker is
    // most likely to reclaim. Sealing is also what interleaves sort work with arrival (fig. 1).
    // 0 disables scaling and pins the target to BUCKET_TARGET_BYTES.
    private static final int BUCKET_COUNT_TARGET = Integer.getInteger("hyracks.sort.bucketCountTarget", 256);

    // [type-cut buckets] Close the current bucket when the sort column's TYPE changes, so every
    // bucket holds one type. This matters because a runtime-detecting key cannot be exact over a
    // mixed column -- a string key is a 4-byte prefix, so equal keys do not imply equal values and
    // every tie must consult the comparator. Partitioned by type, the all-numeric buckets ARE exact
    // and can skip the comparator; cross-type ordering is then handled by the ordering class during
    // the merge, where it is correct by construction. Off by default: it only pays on mixed columns.
    private static final boolean TYPE_CUT_BUCKETS =
            Boolean.parseBoolean(System.getProperty("hyracks.sort.typeCutBuckets", "false"));
    // Guard against pathological input. A FIXED floor is not enough: measured on a column whose type
    // alternates every 1-2 tuples, a 64-tuple floor sealed a bucket every ~64 tuples -- roughly 150k
    // buckets per run at a 512MB budget, and a 28x SLOWDOWN (228s vs 8s). The floor must scale with
    // the bucket target so a type-cut bucket is never much smaller than a normally-sealed one; on
    // constantly-alternating input the cut then degenerates to ordinary size-based sealing.
    private static final int TYPE_CUT_MIN_TUPLES =
            Math.max(1, Integer.getInteger("hyracks.sort.typeCutMinTuples", 1024));
    private static final int TAG_NONE = -1;
    private int currentBucketTag = TAG_NONE; // type tag of the bucket being filled
    // AND of every sealed bucket's exactness. The cascade/merge compares tuples ACROSS buckets, so
    // it may only skip the comparator if EVERY bucket it could touch was exact.
    private boolean runAllExact = true;
    private long bucketTargetBytes = BUCKET_TARGET_BYTES;
    // [U-curve investigation] Seal a bucket after this many TUPLES, if > 0 (0 = use the byte target).
    // Total slot moves for bucket-sort + one k-way merge is about N*(log2(bucketTuples) + 1), so the
    // TUPLE count is what the cost actually depends on; bytes are only a proxy, and the proxy drifts
    // with tuple width and with ptrSize (3..7 ints by key type). Sweeping this directly tests whether
    // smaller buckets profitably trade sort passes for merge fan-in.
    private static final int BUCKET_TARGET_TUPLES = Integer.getInteger("hyracks.sort.bucketTargetTuples", 0);
    private int currentBucketTuples;
    private long currentBucketBytes; // data bytes accumulated in the current, unsealed bucket
    private int builtTuples; // # tuples whose pointers are already built into tPointers
    private int currentBucketStart; // tuple index where the current (unsealed) bucket starts
    private int[] bucketEnds; // end tuple index (exclusive) of each sorted run (a bucket, or merged buckets)
    private int numBuckets; // # sorted runs
    private int[] bucketLevels; // [Stage 3] each run's merge level. Used to merge runs in a cascade.
    // [Stage 3] Cascade fan-in: merge whenever this many newest runs share a level. Spans the whole
    // eager/lazy spectrum: 2 = eager binary cascade; a value above the bucket count never cascades,
    // leaving one merge at the end.
    //
    // NOTE: this MUST read the system property. It was briefly a hardcoded 2, which silently voided
    // a whole fan-in sweep on EC2 (2026-08-30) -- every arm ran fan-in 2 while the harness believed
    // it was varying the knob. The value is echoed on the adaptive-sort-run log line so a run's
    // configuration can always be confirmed from its own output rather than assumed.
    private static final int MERGE_FAN_IN = Math.max(2, Integer.getInteger("hyracks.sort.mergeFanIn", 2));
    // Use a one-pass k-way tournament merge instead of balanced pairwise passes when merging more
    // than two runs. Off by default so it can be A/B'd: -Dhyracks.sort.kwayMerge=true
    private static final boolean KWAY_MERGE = Boolean.getBoolean("hyracks.sort.kwayMerge");
    private int mergeFanIn = MERGE_FAN_IN;
    private int[] tScratch; // scratch buffer for the stable merges (bucket sort + bucket merge)
    // [Stage 2] frames [0, sortedFrameCount) belong to sealed (sorted) buckets; the rest are the unsorted tail
    private int sortedFrameCount;

    private final FrameTupleAccessor fta2;
    private final BufferInfo info = new BufferInfo(null, -1, -1);

    public AbstractFrameSorter(IHyracksTaskContext ctx, IFrameBufferManager bufferManager, int maxSortFrames,
            int[] sortFields, INormalizedKeyComputerFactory[] normalizedKeyComputerFactories,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor, int outputLimit)
            throws HyracksDataException {
        this.bufferManager = bufferManager;
        if (maxSortFrames == VariableFramePool.UNLIMITED_MEMORY) {
            this.maxSortMemory = Long.MAX_VALUE;
        } else {
            this.maxSortMemory = (long) ctx.getInitialFrameSize() * maxSortFrames;
        }
        // The budget is also set here, not only through setMaxSortMemory, so the bucketing policy
        // has to be applied for the FIRST run too -- otherwise a large compile-time budget would
        // still bucket until the broker happened to move the budget.
        applyBucketingPolicy();
        this.sortFields = sortFields;

        int runningNormalizedKeyTotalLength = 0;

        if (normalizedKeyComputerFactories != null) {
            int decisivePrefixLength = NormalizedKeyUtils.getDecisivePrefixLength(normalizedKeyComputerFactories);

            // we only take a prefix of the decisive normalized keys, plus at most indecisive normalized keys
            // ideally, the caller should prepare normalizers in this way, but we just guard here to avoid
            // computing unncessary normalized keys
            int normalizedKeys = decisivePrefixLength < normalizedKeyComputerFactories.length ? decisivePrefixLength + 1
                    : decisivePrefixLength;
            this.nkcs = new INormalizedKeyComputer[normalizedKeys];
            this.normalizedKeyLength = new int[normalizedKeys];

            for (int i = 0; i < normalizedKeys; i++) {
                this.nkcs[i] = normalizedKeyComputerFactories[i].createNormalizedKeyComputer();
                this.normalizedKeyLength[i] =
                        normalizedKeyComputerFactories[i].getNormalizedKeyProperties().getNormalizedKeyLength();
                runningNormalizedKeyTotalLength += this.normalizedKeyLength[i];
            }
            this.normalizedKeysDecisive = decisivePrefixLength == comparatorFactories.length;
        } else {
            this.nkcs = null;
            this.normalizedKeyLength = null;
            this.normalizedKeysDecisive = false;
        }
        this.normalizedKeyTotalLength = runningNormalizedKeyTotalLength;
        this.ptrSize = ID_NORMALIZED_KEY + normalizedKeyTotalLength;
        this.comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        this.inputTupleAccessor = new FrameTupleAccessor(recordDescriptor);
        this.outputAppender = new FrameTupleAppender();
        this.outputFrame = new VSizeFrame(ctx);
        this.outputLimit = outputLimit;
        this.fta2 = new FrameTupleAccessor(recordDescriptor);
        this.tmpPointer = new int[ptrSize];
        // [U-curve investigation] What the sort actually got for keys. This decides the cost of every
        // single comparison:
        //   nkcs=none  -> NO normalized key. Every compare() dereferences tuple bytes through
        //                 bufferManager.getFrame() and runs the full binary comparator. This happens
        //                 when the sort key's static type is unknown -- e.g. an UNDECLARED field of an
        //                 `open` type, whose type is ANY, which hits `default: return null` in
        //                 NormalizedKeyComputerFactoryProvider.
        //   decisive=true  -> comparisons stop at the normalized key; tuple bytes are never touched.
        //   decisive=false -> normalized key is only a prefix (e.g. UTF8 strings get ONE int) and ties
        //                 fall through to the comparator.
        // ptrSize = 3 + normalizedKeyTotalLength, so ptrSize==3 means no normalized key at all.
        if (LOGGER.isInfoEnabled()) {
            LOGGER.warn("adaptive-sort-keys: ptrSize={} nkcs={} nkTotalLen={} decisive={} comparators={}", ptrSize,
                    nkcs == null ? "none" : String.valueOf(nkcs.length), normalizedKeyTotalLength,
                    normalizedKeysDecisive, comparators.length);
        }
    }

    // [ADDED for memory-adaptive sort]
    // Change the in-memory budget gate (checked first in insertFrame) so the budget can move at runtime.
    @Override
    public void setMaxSortMemory(long maxSortMemory) {
        this.maxSortMemory = maxSortMemory;
        applyBucketingPolicy();
    }

    /**
     * Size buckets so their COUNT per run stays roughly constant as the budget changes, instead of
     * letting a fixed byte target multiply buckets without limit. Re-evaluated on every budget
     * change, because the budget moves at runtime.
     */
    /**
     * Smallest bucket a type change may close: at least half a normal bucket. Type-cutting can then
     * at most double the bucket count, never explode it.
     */
    private int minTypeCutTuples() {
        long avgTupleBytes = tupleCount > 0 ? Math.max(1, totalMemoryUsed / tupleCount) : 0;
        long targetTuples = avgTupleBytes > 0 ? bucketTargetBytes / avgTupleBytes : 0;
        return (int) Math.max(TYPE_CUT_MIN_TUPLES, Math.min(Integer.MAX_VALUE, targetTuples / 2));
    }

    private void applyBucketingPolicy() {
        if (BUCKET_COUNT_TARGET <= 0 || maxSortMemory <= 0 || maxSortMemory == Long.MAX_VALUE) {
            bucketTargetBytes = BUCKET_TARGET_BYTES;
            return;
        }
        // Grow the bucket with the budget so the count stays near BUCKET_COUNT_TARGET, but never
        // shrink below the cache-sized floor -- a small budget should still get cache-sized buckets.
        bucketTargetBytes = Math.max(BUCKET_TARGET_BYTES, maxSortMemory / BUCKET_COUNT_TARGET);
    }

    // [ADDED for memory-adaptive sort] bytes currently held by this run (used-vs-budget decisions).
    @Override
    public long getUsedMemory() {
        return totalMemoryUsed;
    }

    // [ADDED for memory-adaptive sort] total loaded tuples, and how many are already sorted. Tuples in
    // sealed buckets are sorted (currentBucketStart of them); [currentBucketStart, tupleCount) is the
    // current unsealed (not-yet-sorted) bucket. Used to build the operator's 3-tier MemoryStatus.
    @Override
    public int getTupleCount() {
        return tupleCount;
    }

    @Override
    public int getSortedTupleCount() {
        return currentBucketStart;
    }

    @Override
    public void reset() throws HyracksDataException {
        this.tupleCount = 0;
        this.totalMemoryUsed = 0;
        this.bufferManager.reset();
        // [Stage 1] reset bucket state for the next run (tPointers/tScratch are kept, grow-only)
        this.builtTuples = 0;
        this.currentBucketStart = 0;
        this.currentBucketBytes = 0;
        this.currentBucketTuples = 0;
        this.nkUsable = true;
        this.runtimeDecisive = false;
        this.currentBucketTag = TAG_NONE;
        this.runAllExact = true;
        this.numBuckets = 0;
        this.sortedFrameCount = 0;
        // phase counters are per-run: clear them so each run reports its own timeline
        this.phLoadNs = 0;
        this.phSortNs = 0;
        this.phMergeNs = 0;
        this.phSortEvents = 0;
        this.phRunStartNs = 0;
        this.phFirstSortAtNs = 0;
        this.phLastSortAtNs = 0;
        this.phCascadeNs = 0;
        this.phSortCallNs = 0;
        this.phFlushNs = 0;
        this.phMoves = 0;
        this.phCompares = 0;
        this.phInsertNs = 0;
        this.phGapNs = 0;
        this.phLastInsertEndNs = 0;
        this.phLastSampleNs = 0;
        this.phRunSeq++;
    }

    @Override
    public boolean insertFrame(ByteBuffer inputBuffer) throws HyracksDataException {
        inputTupleAccessor.reset(inputBuffer);
        if (inputTupleAccessor.getTupleCount() <= 0) {
            return true;
        }
        long requiredMemory = getRequiredMemory(inputTupleAccessor);
        // [Stage 1] on accept, immediately build this frame's pointers and, once a bucket fills, sort
        // it -- so sort CPU overlaps input arrival instead of spiking all at once at flush time.
        if (totalMemoryUsed + requiredMemory <= maxSortMemory) {
            int inserted = inputTupleAccessor.getTupleCount(); // read before buildFramePointers repoints the accessor
            long tEnter = PHASE_LOG ? System.nanoTime() : 0;
            if (PHASE_LOG) {
                if (phRunStartNs == 0) {
                    phRunStartNs = tEnter;
                    phLastSampleNs = tEnter;
                } else if (phLastInsertEndNs != 0) {
                    phGapNs += tEnter - phLastInsertEndNs; // time spent outside the sorter
                }
            }
            int frameIndex = bufferManager.insertFrame(inputBuffer);
            if (frameIndex >= 0) {
                long t0 = PHASE_LOG ? System.nanoTime() : 0;
                totalMemoryUsed += requiredMemory;
                tupleCount += inserted;
                buildFramePointers(frameIndex);
                currentBucketBytes += inputBuffer.capacity();
                currentBucketTuples += inserted;
                if (PHASE_LOG) {
                    phLoadNs += System.nanoTime() - t0;
                }
                boolean seal = BUCKET_TARGET_TUPLES > 0 ? currentBucketTuples >= BUCKET_TARGET_TUPLES
                        : currentBucketBytes >= bucketTargetBytes;
                if (seal) {
                    sealBucket();
                }
                if (PHASE_LOG) {
                    long tEnd = System.nanoTime();
                    phInsertNs += tEnd - tEnter;
                    phLastInsertEndNs = tEnd;
                    if (tEnd - phLastSampleNs >= PHASE_SAMPLE_NS) {
                        phLastSampleNs = tEnd;
                        LOGGER.warn(
                                "adaptive-sort-series: run={} tRelMs={} memUsed={} tuples={} frames={} "
                                        + "buckets={} cumSortNs={} cumMergeNs={} cumLoadNs={} "
                                        + "cumInsertNs={} cumGapNs={} cumCascadeNs={} sortEvents={}",
                                phRunSeq, (tEnd - phRunStartNs) / 1000000L, totalMemoryUsed, tupleCount,
                                getFrameCount(), numBuckets, phSortNs, phMergeNs, phLoadNs, phInsertNs, phGapNs,
                                phCascadeNs, phSortEvents);
                    }
                }
                return true;
            }
        }
        if (getFrameCount() == 0) {
            throw HyracksDataException.create(ErrorCode.FRAME_BIGGER_THAN_SORT_MEMORY,
                    inputTupleAccessor.getBuffer().capacity(), requiredMemory, totalMemoryUsed, maxSortMemory);
        }
        return false;
    }

    protected long getRequiredMemory(FrameTupleAccessor frameAccessor) {
        // The stable merges also need a scratch array same size as tPointers, so reserve 2x the pointer memory.
        // ORIGINAL: frame bytes + ONE pointer array (tPointers):
        //   return (long) frameAccessor.getBuffer().capacity() + ptrSize * frameAccessor.getTupleCount() * Integer.BYTES;
        return (long) frameAccessor.getBuffer().capacity()
                + 2L * ptrSize * frameAccessor.getTupleCount() * Integer.BYTES;
    }

    @Override
    public void sort() throws HyracksDataException {
        long tSortCall0 = PHASE_LOG ? System.nanoTime() : 0;
        refreshRuntimeDecisive();
        // [ADDED for memory-adaptive sort] original AsterixDB sort() had NO logging block
        if (LOGGER.isInfoEnabled()) {
            long fillPct = (maxSortMemory > 0 && maxSortMemory != Long.MAX_VALUE)
                    ? (100L * totalMemoryUsed / maxSortMemory) : -1;
            LOGGER.warn(
                    "adaptive-sort-run: framesLoaded={} bytesUsed={} budgetBytes={} fillPct={} tuples={} "
                            + "mergeFanIn={} bucketTargetBytes={} runtimeDecisive={}",
                    getFrameCount(), totalMemoryUsed, maxSortMemory, fillPct, tupleCount, mergeFanIn, bucketTargetBytes,
                    runtimeDecisive);
        }
        // ORIGINAL sort(): build ONE tPointers array over ALL frames, then sort the whole thing in one pass.
        // Now, pointers are built incrementally in insertFrame and full buckets are already sorted.
        // Here we just seal the last bucket and merge the buckets.

        if (!nkUsable && numBuckets > 0) {
            // Buckets were sorted while the (now-invalid) normalized keys were still trusted, so
            // their internal order cannot be relied upon. Discard the bucket structure and sort the
            // whole run again -- compare() now ignores the keys, so this pass is comparator-only.
            // Rare by construction: it needs a genuinely mixed-type sort column.
            LOGGER.warn("adaptive-sort: normalized keys invalidated (heterogeneous sort column); "
                    + "re-sorting {} tuples with the comparator alone", builtTuples);
            numBuckets = 0;
            currentBucketStart = 0;
            currentBucketBytes = 0;
            currentBucketTuples = 0;
            sortBucketSlice(0, builtTuples);
            pushRun(builtTuples, 0);
            currentBucketStart = builtTuples;
            return;
        }
        sealBucket();
        if (numBuckets > 1) {
            long t0 = PHASE_LOG ? System.nanoTime() : 0;
            mergeBuckets();
            if (PHASE_LOG) {
                phMergeNs += System.nanoTime() - t0;
            }
        }
        if (PHASE_LOG) {
            phSortCallNs += System.nanoTime() - tSortCall0;
            long span = phLastSortAtNs - phFirstSortAtNs;
            long runSpan = System.nanoTime() - phRunStartNs;
            // spreadPct = what fraction of the run's lifetime the sorting was spread over.
            // ~0 means all sorting happened at one instant (load-everything-then-sort);
            // approaching 100 means sort work tracked data arrival.
            long spreadPct = runSpan > 0 ? (100L * span / runSpan) : 0;
            long residual = runSpan - (phGapNs + phInsertNs + phSortCallNs);
            LOGGER.warn(
                    "adaptive-sort-phase: run={} loadNs={} sortNs={} cascadeNs={} mergeNs={} "
                            + "sortCallNs={} flushNs={} residualNs={} moves={} compares={} sortEvents={} "
                            + "firstSortAtNs={} lastSortAtNs={} runSpanNs={} spreadPct={} insertNs={} "
                            + "gapNs={} tuples={} frames={} bucketBytes={} bucketTuples={} fanIn={} kway={}",
                    phRunSeq, phLoadNs, phSortNs, phCascadeNs, phMergeNs, phSortCallNs, phFlushNs, residual, phMoves,
                    phCompares, phSortEvents, phFirstSortAtNs, phLastSortAtNs, runSpan, spreadPct, phInsertNs, phGapNs,
                    tupleCount, getFrameCount(), bucketTargetBytes, BUCKET_TARGET_TUPLES, mergeFanIn, KWAY_MERGE);
        }
    }

    // [Stage 1] Build this frame's tuple pointers (frameIndex, start, end, normalized keys) and append
    // them to tPointers at [builtTuples, builtTuples + tCount). Extracted from the original sort() loop
    // so it can run per-frame as data arrives, rather than once over all frames at flush.
    private void buildFramePointers(int frameIndex) throws HyracksDataException {
        bufferManager.getFrame(frameIndex, info);
        inputTupleAccessor.reset(info.getBuffer(), info.getStartOffset(), info.getLength());
        int tCount = inputTupleAccessor.getTupleCount();
        byte[] array = inputTupleAccessor.getBuffer().array(); // reassigned after a mid-loop seal
        int fieldSlotsLength = inputTupleAccessor.getFieldSlotsLength();
        ensureTPointersCapacity(builtTuples + tCount);
        for (int j = 0; j < tCount; ++j) {
            int tStart = inputTupleAccessor.getTupleStartOffset(j);
            int tEnd = inputTupleAccessor.getTupleEndOffset(j);
            if (TYPE_CUT_BUCKETS && nkcs != null) {
                // Read the sort column's type tag before this tuple joins the bucket. Sealing here
                // keeps every bucket single-typed, which is what lets the key be exact within it.
                int tagOff = inputTupleAccessor.getFieldStartOffset(j, sortFields[0]) + tStart + fieldSlotsLength;
                int tag = array[tagOff] & 0xff;
                if (currentBucketTag != TAG_NONE && tag != currentBucketTag
                        && builtTuples - currentBucketStart >= minTypeCutTuples()) {
                    sealBucket(); // seals [currentBucketStart, builtTuples): everything before this tuple
                    // The capacity check ran before this loop; a mid-frame seal can touch the
                    // pointer arrays, so re-assert room for the tuples still to come in this frame.
                    ensureTPointersCapacity(builtTuples + (tCount - j));
                    // CRITICAL: sealing sorts, and compare() re-points the SHARED inputTupleAccessor
                    // at whichever frame it is comparing. Without restoring it here the rest of this
                    // loop reads tuple offsets from the wrong frame -- which is an
                    // ArrayIndexOutOfBoundsException when we are lucky and silent corruption when
                    // we are not.
                    bufferManager.getFrame(frameIndex, info);
                    inputTupleAccessor.reset(info.getBuffer(), info.getStartOffset(), info.getLength());
                    array = inputTupleAccessor.getBuffer().array();
                }
                if (currentBucketTag == TAG_NONE) {
                    currentBucketTag = tag;
                }
            }
            int ptr = builtTuples++;
            tPointers[ptr * ptrSize + ID_FRAME_ID] = frameIndex;
            tPointers[ptr * ptrSize + ID_TUPLE_START] = tStart;
            tPointers[ptr * ptrSize + ID_TUPLE_END] = tEnd;
            if (nkcs == null) {
                continue;
            }
            if (nkUsable && !nkcs[0].isKeyValid()) {
                nkUsable = false; // column turned out to be heterogeneous
                // Log HERE, at the moment the key is abandoned. The re-sort path below also logs,
                // but only when buckets were already sealed (numBuckets > 0); a sort small enough
                // to sit in one unsealed bucket would fall back correctly and silently, which made
                // the invalidation look like it never happened.
                LOGGER.warn("adaptive-sort: normalized key abandoned at tuple {} (heterogeneous "
                        + "sort column); ordering falls back to the comparator", builtTuples);
            }
            int keyPos = ptr * ptrSize + ID_NORMALIZED_KEY;
            for (int k = 0; k < nkcs.length; k++) {
                int sortField = sortFields[k];
                int fieldStartOffsetRel = inputTupleAccessor.getFieldStartOffset(j, sortField);
                int fieldEndOffsetRel = inputTupleAccessor.getFieldEndOffset(j, sortField);
                int fieldStartOffset = fieldStartOffsetRel + tStart + fieldSlotsLength;
                nkcs[k].normalize(array, fieldStartOffset, fieldEndOffsetRel - fieldStartOffsetRel, tPointers, keyPos);
                keyPos += normalizedKeyLength[k];
            }
        }
    }

    // [Stage 1] Stably sort the current unsealed bucket [currentBucketStart, builtTuples), record its
    // boundary, and start a new bucket. No-op if the bucket is empty.
    private void sealBucket() throws HyracksDataException {
        int len = builtTuples - currentBucketStart;
        if (len > 0) {
            long t0 = PHASE_LOG ? System.nanoTime() : 0;
            sortBucketSlice(currentBucketStart, len);
            if (PHASE_LOG) {
                long now = System.nanoTime();
                phSortNs += now - t0;
                phSortEvents++;
                // WHEN sorting happened, relative to the run's first frame. Interleaved sorting
                // spreads these across the run; load-then-sort collapses them all to the end.
                long at = t0 - phRunStartNs;
                if (phSortEvents == 1) {
                    phFirstSortAtNs = at;
                }
                phLastSortAtNs = at;
            }
            // [Stage 3] a freshly sorted bucket is a level-0 run. Then, while the mergeFanIn newest runs are
            // all the same size, merge them into one run of the next level up
            pushRun(builtTuples, 0);
            // Fold this bucket's exactness into the run-level flag BEFORE any cross-bucket merge,
            // then switch the cascade to the conservative across-buckets rule.
            if (nkcs != null) {
                runAllExact &= nkcs[0].isKeyExact();
            }
            refreshRuntimeDecisiveAcrossBuckets();
            long tc0 = PHASE_LOG ? System.nanoTime() : 0;
            while (numBuckets >= mergeFanIn && topRunsShareLevel(mergeFanIn)) {
                mergeTopRuns(mergeFanIn);
            }
            if (PHASE_LOG) {
                phCascadeNs += System.nanoTime() - tc0;
            }
            currentBucketStart = builtTuples;
            sortedFrameCount = getFrameCount(); // every frame so far is now in a sealed, sorted run
        }
        currentBucketBytes = 0;
        currentBucketTuples = 0;
        currentBucketTag = TAG_NONE;
        if (nkcs != null) {
            nkcs[0].resetKeyEpoch(); // the next bucket's exactness is judged on its own
        }
    }

    // [Stage 3] record a new sorted run: its end tuple index and its merge level (size).
    private void pushRun(int end, int level) {
        if (bucketEnds == null) {
            bucketEnds = new int[16];
            bucketLevels = new int[16];
        } else if (numBuckets >= bucketEnds.length) {
            int[] grownEnds = new int[bucketEnds.length * 2];
            int[] grownLevels = new int[bucketLevels.length * 2];
            System.arraycopy(bucketEnds, 0, grownEnds, 0, numBuckets);
            System.arraycopy(bucketLevels, 0, grownLevels, 0, numBuckets);
            bucketEnds = grownEnds;
            bucketLevels = grownLevels;
        }
        bucketEnds[numBuckets] = end;
        bucketLevels[numBuckets] = level;
        numBuckets++;
    }

    // Merge `count` adjacent sorted runs in ONE pass using a tournament (loser-tree) over the run
    // heads, instead of ceil(log2(count)) balanced pairwise passes.
    //
    // Both do the same N*log2(k) comparisons. The difference is data movement:
    //   balanced pairwise : every pass rewrites every tuple  -> N * log2(k) slot moves
    //   k-way tournament  : each tuple is written exactly once -> N slot moves
    // At k=325 runs that is ~8x less pointer traffic, and a slot is ptrSize ints (20-28 bytes), so
    // movement is where a wide pointer array actually hurts.
    //
    // STABLE: ties are broken by run index, and runs are indexed in input order, so the earlier
    // tuple always wins -- the same guarantee mergeRange gives with `cmp <= 0`.
    private void mergeRunsKWay(int first, int count, int lo, int hi) throws HyracksDataException {
        ensureScratchCapacity(hi);
        // head[i] = next unread tuple index of run i; end[i] = its exclusive end
        int[] head = new int[count];
        int[] end = new int[count];
        int start = lo;
        for (int i = 0; i < count; i++) {
            head[i] = start;
            end[i] = bucketEnds[first + i];
            start = end[i];
        }
        // Winner tree over `count` slots: tree[1] is the overall winner. Leaves live at [size, 2*size).
        int size = Integer.highestOneBit(Math.max(1, count - 1)) << 1;
        if (size < count) {
            size <<= 1;
        }
        int[] tree = new int[size * 2];
        for (int i = 0; i < size; i++) {
            tree[size + i] = (i < count && head[i] < end[i]) ? i : -1;
        }
        for (int i = size - 1; i >= 1; i--) {
            tree[i] = pickWinner(tree[2 * i], tree[2 * i + 1], head);
        }
        int w = lo;
        while (tree[1] >= 0) {
            int run = tree[1];
            copySlot(tPointers, head[run]++, tScratch, w++);
            if (head[run] >= end[run]) {
                tree[size + run] = -1; // run exhausted
            }
            // replay only the path from this leaf to the root: log2(k) comparisons, no full rebuild
            for (int node = (size + run) >> 1; node >= 1; node >>= 1) {
                tree[node] = pickWinner(tree[2 * node], tree[2 * node + 1], head);
            }
        }
        // one copy back; the merge itself wrote each tuple exactly once
        if (PHASE_LOG) {
            phMoves += hi - lo;
        }
        System.arraycopy(tScratch, lo * ptrSize, tPointers, lo * ptrSize, (hi - lo) * ptrSize);
        bucketEnds[first] = hi;
        bucketLevels[first]++;
        numBuckets = first + 1;
    }

    /** Lower run index wins ties, which is what keeps the k-way merge stable. */
    private int pickWinner(int a, int b, int[] head) throws HyracksDataException {
        if (a < 0) {
            return b;
        }
        if (b < 0) {
            return a;
        }
        int cmp = compare(tPointers, head[a], tPointers, head[b]);
        if (cmp < 0) {
            return a;
        }
        if (cmp > 0) {
            return b;
        }
        return a < b ? a : b;
    }

    // [Stage 3] Do the `count` newest runs all sit at the same merge level? (cascade trigger)
    private boolean topRunsShareLevel(int count) {
        int level = bucketLevels[numBuckets - 1];
        for (int i = numBuckets - count; i < numBuckets - 1; i++) {
            if (bucketLevels[i] != level) {
                return false;
            }
        }
        return true;
    }

    // [Stage 3] Merge the `count` newest (adjacent) sorted runs into a single sorted run, in place in
    // tPointers, via balanced pairwise passes ping-ponging with tScratch. STABLE. Collapses the count run
    // entries into one whose level is one higher. count == numBuckets merges everything sealed so far.
    private void mergeTopRuns(int count) throws HyracksDataException {
        int first = numBuckets - count;
        int lo = first > 0 ? bucketEnds[first - 1] : 0;
        int hi = bucketEnds[numBuckets - 1];
        if (KWAY_MERGE && count > 2) {
            mergeRunsKWay(first, count, lo, hi);
            return;
        }
        ensureScratchCapacity(hi);
        int[] ends = new int[count];
        System.arraycopy(bucketEnds, first, ends, 0, count);
        int runCount = count;
        int[] src = tPointers;
        int[] dst = tScratch;
        boolean inScratch = false;
        while (runCount > 1) {
            int[] newEnds = new int[(runCount + 1) >> 1];
            int w = 0;
            int start = lo;
            int r = 0;
            while (r < runCount) {
                int mid = ends[r];
                if (r + 1 < runCount) {
                    int end = ends[r + 1];
                    mergeRange(src, dst, start, mid, end);
                    newEnds[w++] = end;
                    start = end;
                    r += 2;
                } else {
                    // odd run left over: copy it verbatim so the ref-swap below keeps it
                    System.arraycopy(src, start * ptrSize, dst, start * ptrSize, (mid - start) * ptrSize);
                    newEnds[w++] = mid;
                    start = mid;
                    r += 1;
                }
            }
            int[] t = src;
            src = dst;
            dst = t;
            ends = newEnds;
            runCount = w;
            inScratch = !inScratch;
        }
        if (inScratch) {
            // the merged result ended up in tScratch; copy just this group's span back to tPointers
            if (PHASE_LOG) {
                phMoves += hi - lo;
            }
            System.arraycopy(src, lo * ptrSize, tPointers, lo * ptrSize, (hi - lo) * ptrSize);
        }
        bucketEnds[first] = hi; // the merged run now spans [lo, hi)
        bucketLevels[first]++;
        numBuckets = first + 1;
    }

    // [Stage 1] Slice-safe, STABLE bottom-up merge sort of tPointers[offset, offset+length), using
    // tScratch as scratch. Only touches [offset, offset+length) in either array (local src/dst refs,
    // not a whole-array swap), so already-sorted buckets elsewhere in tPointers are left intact.
    private void sortBucketSlice(int offset, int length) throws HyracksDataException {
        if (length <= 1) {
            return;
        }
        // Buckets are sorted incrementally, mid-insert, so a later tuple can still invalidate the
        // key or make it inexact. Re-derive per bucket rather than trusting an earlier answer.
        refreshRuntimeDecisive();
        // NOTE (2026-08-30): an attempt to remove the trailing copy-back -- insertion-sort small
        // blocks in place, then pick a block size making the merge pass count even -- measured 3.6%
        // SLOWER than this version (20.84s vs 20.11s at 512MB). Insertion sort shifts elements with
        // copySlot, moving ptrSize ints (20-28 bytes) per shift: ~5-7x more data movement than the
        // merge passes it replaced. Insertion sort only wins when elements are small and comparisons
        // dominate; neither holds for a pointer array this wide. Do not retry without measuring.
        ensureScratchCapacity(offset + length);
        int end = offset + length;
        int[] src = tPointers;
        int[] dst = tScratch;
        boolean inScratch = false;
        for (int step = 1; step < length; step <<= 1) {
            for (int i = offset; i < end; i += step << 1) {
                int mid = Math.min(i + step, end);
                int hi = Math.min(i + (step << 1), end);
                mergeRange(src, dst, i, mid, hi);
            }
            int[] t = src;
            src = dst;
            dst = t;
            inScratch = !inScratch;
        }
        if (inScratch) {
            // the sorted result ended up in tScratch; copy just this slice back to tPointers
            if (PHASE_LOG) {
                phMoves += length;
            }
            System.arraycopy(src, offset * ptrSize, tPointers, offset * ptrSize, length * ptrSize);
        }
    }

    // [Stage 1] Stably merge the sorted buckets (runs delimited by bucketEnds) into a fully sorted
    // tPointers[0, tupleCount), via balanced pairwise passes. Called only when numBuckets > 1.
    private void mergeBuckets() throws HyracksDataException {
        mergeTopRuns(numBuckets);
    }

    // [Stage 1] Merge two adjacent sorted runs src[lo,mid) and src[mid,hi) into dst[lo,hi). STABLE:
    // on a tie the left (earlier-input) tuple is taken first (cmp <= 0).
    private void mergeRange(int[] src, int[] dst, int lo, int mid, int hi) throws HyracksDataException {
        int i = lo;
        int j = mid;
        int k = lo;
        while (i < mid && j < hi) {
            if (compare(src, i, src, j) <= 0) {
                copySlot(src, i++, dst, k++);
            } else {
                copySlot(src, j++, dst, k++);
            }
        }
        while (i < mid) {
            copySlot(src, i++, dst, k++);
        }
        while (j < hi) {
            copySlot(src, j++, dst, k++);
        }
    }

    private void copySlot(int[] src, int srcTuple, int[] dst, int dstTuple) {
        if (PHASE_LOG) {
            phMoves++;
        }
        System.arraycopy(src, srcTuple * ptrSize, dst, dstTuple * ptrSize, ptrSize);
    }

    private void ensureTPointersCapacity(int neededTuples) {
        int need = neededTuples * ptrSize;
        if (tPointers != null && tPointers.length >= need) {
            return;
        }
        int newLen = tPointers == null ? Math.max(need, 1024 * ptrSize) : tPointers.length;
        while (newLen < need) {
            newLen <<= 1;
        }
        int[] grown = new int[newLen];
        if (tPointers != null && builtTuples > 0) {
            System.arraycopy(tPointers, 0, grown, 0, builtTuples * ptrSize);
        }
        tPointers = grown;
    }

    private void ensureScratchCapacity(int neededTuples) {
        int need = neededTuples * ptrSize;
        if (tScratch == null || tScratch.length < need) {
            tScratch = new int[need];
        }
    }

    // Original per-algorithm whole-array sort hook. No longer called by Stage 1's sort() (which uses a
    // unified stable bucket merge), but kept implemented by the subclasses for reference/presentation.
    abstract void sortTupleReferences() throws HyracksDataException;

    @Override
    public int getFrameCount() {
        return bufferManager.getNumFrames();
    }

    @Override
    public boolean hasRemaining() {
        return getFrameCount() > 0;
    }

    @Override
    public int flush(IFrameWriter writer) throws HyracksDataException {
        // flush the whole sorted run (respecting a top-K output limit, if any)
        return flushTuples(writer, Math.min(tupleCount, outputLimit));
    }

    // [Stage 2] Write out the first `limit` tuples of tPointers, in sorted order. flush() sends all of
    // them; a partial spill (spillSortedKeepUnsorted) sends only the already-sorted prefix.
    private int flushTuples(IFrameWriter writer, int limit) throws HyracksDataException {
        long tFlush0 = PHASE_LOG ? System.nanoTime() : 0;
        outputAppender.reset(outputFrame, true);
        int maxFrameSize = outputFrame.getFrameSize();
        int io = 0;
        for (int ptr = 0; ptr < limit; ++ptr) {
            int i = tPointers[ptr * ptrSize + ID_FRAME_ID];
            int tStart = tPointers[ptr * ptrSize + ID_TUPLE_START];
            int tEnd = tPointers[ptr * ptrSize + ID_TUPLE_END];
            bufferManager.getFrame(i, info);
            inputTupleAccessor.reset(info.getBuffer(), info.getStartOffset(), info.getLength());
            int flushed = FrameUtils.appendToWriter(writer, outputAppender, inputTupleAccessor, tStart, tEnd);
            if (flushed > 0) {
                maxFrameSize = Math.max(maxFrameSize, flushed);
                io++;
            }
        }
        maxFrameSize = Math.max(maxFrameSize, outputFrame.getFrameSize());
        outputAppender.write(writer, true);
        if (PHASE_LOG) {
            phFlushNs += System.nanoTime() - tFlush0;
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(
                    "Flushed records:" + limit + " out of " + tupleCount + "; Flushed through " + (io + 1) + " frames");
        }
        return maxFrameSize;
    }

    // [Stage 2] Give memory back on a victim without redoing work: spill only the already-sorted buckets
    // as a run and keep the still-unsorted tail (the current, unsealed bucket) in memory. Returns false
    // if nothing has been sealed/sorted yet, so the caller can just spill everything instead.

    // IMPORTANT: just do sorter.sort(). no need for all this. For 256kb... why?

    @Override
    public boolean spillSortedKeepUnsorted(IFrameWriter writer) throws HyracksDataException {
        if (numBuckets == 0) {
            return false;
        }
        // 1. Merge the sealed buckets into one sorted run, then write it out.
        if (numBuckets > 1) {
            mergeBuckets();
        }
        flushTuples(writer, currentBucketStart);
        int firstTailFrame = sortedFrameCount;
        int frameCount = getFrameCount();
        ByteBuffer[] tail = new ByteBuffer[frameCount - firstTailFrame];
        for (int i = firstTailFrame; i < frameCount; i++) {
            bufferManager.getFrame(i, info);
            int len = info.getLength();
            ByteBuffer copy = ByteBuffer.allocate(len);
            System.arraycopy(info.getBuffer().array(), info.getStartOffset(), copy.array(), 0, len);
            tail[i - firstTailFrame] = copy;
        }
        reset();
        for (ByteBuffer f : tail) {
            insertFrame(f);
        }
        return true;
    }

    /**
     * Re-derive {@link #runtimeDecisive}. Safe only at a sort entry point: inserting more tuples can
     * invalidate the key (a new type tag) or make it inexact, so this must never be cached across
     * inserts. Requires a single sort column -- with more, equal first keys say nothing about the
     * remaining ones.
     */
    private void refreshRuntimeDecisive() {
        runtimeDecisive = decisiveBase() && nkcs[0].isKeyExact();
    }

    /**
     * Decisiveness for comparisons that span buckets (the cascade and the final merge). A bucket may
     * be exact while its neighbour is not, so crossing comparisons may only skip the comparator when
     * EVERY sealed bucket was exact.
     */
    private void refreshRuntimeDecisiveAcrossBuckets() {
        runtimeDecisive = decisiveBase() && runAllExact && nkcs[0].isKeyExact();
    }

    private boolean decisiveBase() {
        return nkcs != null && nkUsable && nkcs.length == 1 && comparators.length == 1;
    }

    protected final int compare(int tp1, int tp2) throws HyracksDataException {
        return compare(tPointers, tp1, tPointers, tp2);
    }

    protected final int compare(int[] tPointers1, int tp1, int[] tPointers2, int tp2) throws HyracksDataException {
        if (PHASE_LOG) {
            phCompares++;
        }
        if (nkcs != null && nkUsable) {
            int cmpNormalizedKey =
                    NormalizedKeyUtils.compareNormalizeKeys(tPointers1, tp1 * ptrSize + ID_NORMALIZED_KEY, tPointers2,
                            tp2 * ptrSize + ID_NORMALIZED_KEY, normalizedKeyTotalLength);
            if (cmpNormalizedKey != 0 || normalizedKeysDecisive || runtimeDecisive) {
                return cmpNormalizedKey;
            }
        }

        int i1 = tPointers1[tp1 * ptrSize + ID_FRAME_ID];
        int j1 = tPointers1[tp1 * ptrSize + ID_TUPLE_START];
        int i2 = tPointers2[tp2 * ptrSize + ID_FRAME_ID];
        int j2 = tPointers2[tp2 * ptrSize + ID_TUPLE_START];

        bufferManager.getFrame(i1, info);
        byte[] b1 = info.getBuffer().array();
        inputTupleAccessor.reset(info.getBuffer(), info.getStartOffset(), info.getLength());

        bufferManager.getFrame(i2, info);
        byte[] b2 = info.getBuffer().array();
        fta2.reset(info.getBuffer(), info.getStartOffset(), info.getLength());
        for (int f = 0; f < comparators.length; ++f) {
            int fIdx = sortFields[f];
            int f1Start = fIdx == 0 ? 0 : IntSerDeUtils.getInt(b1, j1 + (fIdx - 1) * 4);
            int f1End = IntSerDeUtils.getInt(b1, j1 + fIdx * 4);
            int s1 = j1 + inputTupleAccessor.getFieldSlotsLength() + f1Start;
            int l1 = f1End - f1Start;
            int f2Start = fIdx == 0 ? 0 : IntSerDeUtils.getInt(b2, j2 + (fIdx - 1) * 4);
            int f2End = IntSerDeUtils.getInt(b2, j2 + fIdx * 4);
            int s2 = j2 + fta2.getFieldSlotsLength() + f2Start;
            int l2 = f2End - f2Start;
            int c = comparators[f].compare(b1, s1, l1, b2, s2, l2);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    @Override
    public void close() {
        tupleCount = 0;
        totalMemoryUsed = 0;
        bufferManager.close();
        tPointers = null;
        // [Stage 1] release bucket scratch/state
        tScratch = null;
        bucketEnds = null;
        bucketLevels = null;
        builtTuples = 0;
        numBuckets = 0;
        currentBucketStart = 0;
        currentBucketBytes = 0;
        sortedFrameCount = 0;
    }
}
