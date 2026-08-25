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

    // [ADDED] Incremental cache-sized bucket sort. builds each frame's pointers as they arrive.
    private static final long BUCKET_TARGET_BYTES = 256L * 1024; // ~L2 cache; tunable
    private long bucketTargetBytes = BUCKET_TARGET_BYTES;
    private long currentBucketBytes; // data bytes accumulated in the current, unsealed bucket
    private int builtTuples; // # tuples whose pointers are already built into tPointers
    private int currentBucketStart; // tuple index where the current (unsealed) bucket starts
    private int[] bucketEnds; // end tuple index (exclusive) of each sorted run (a bucket, or merged buckets)
    private int numBuckets; // # sorted runs
    private int[] bucketLevels; // [Stage 3] each run's merge level. Used to merge runs in a cascade.
    private static final int MERGE_FAN_IN = 2; // [Stage 3] Cascade fan-in: merge whenever this many newest runs share a level.
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
    }

    // [ADDED for memory-adaptive sort]
    // Change the in-memory budget gate (checked first in insertFrame) so the budget can move at runtime.
    @Override
    public void setMaxSortMemory(long maxSortMemory) {
        this.maxSortMemory = maxSortMemory;
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
        this.numBuckets = 0;
        this.sortedFrameCount = 0;
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
            int frameIndex = bufferManager.insertFrame(inputBuffer);
            if (frameIndex >= 0) {
                totalMemoryUsed += requiredMemory;
                tupleCount += inserted;
                buildFramePointers(frameIndex);
                currentBucketBytes += inputBuffer.capacity();
                if (currentBucketBytes >= bucketTargetBytes) {
                    sealBucket();
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
        // [ADDED for memory-adaptive sort] original AsterixDB sort() had NO logging block
        if (LOGGER.isInfoEnabled()) {
            long fillPct = (maxSortMemory > 0 && maxSortMemory != Long.MAX_VALUE)
                    ? (100L * totalMemoryUsed / maxSortMemory) : -1;
            LOGGER.warn(
                    "adaptive-sort-run: framesLoaded={} bytesUsed={} budgetBytes={} fillPct={} tuples={} "
                            + "mergeFanIn={} bucketTargetBytes={}",
                    getFrameCount(), totalMemoryUsed, maxSortMemory, fillPct, tupleCount, mergeFanIn,
                    bucketTargetBytes);
        }
        // ORIGINAL sort(): build ONE tPointers array over ALL frames, then sort the whole thing in one pass.
        // Now, pointers are built incrementally in insertFrame and full buckets are already sorted.
        // Here we just seal the last bucket and merge the buckets.

        sealBucket();
        if (numBuckets > 1) {
            mergeBuckets();
        }
    }

    // [Stage 1] Build this frame's tuple pointers (frameIndex, start, end, normalized keys) and append
    // them to tPointers at [builtTuples, builtTuples + tCount). Extracted from the original sort() loop
    // so it can run per-frame as data arrives, rather than once over all frames at flush.
    private void buildFramePointers(int frameIndex) throws HyracksDataException {
        bufferManager.getFrame(frameIndex, info);
        inputTupleAccessor.reset(info.getBuffer(), info.getStartOffset(), info.getLength());
        int tCount = inputTupleAccessor.getTupleCount();
        byte[] array = inputTupleAccessor.getBuffer().array();
        int fieldSlotsLength = inputTupleAccessor.getFieldSlotsLength();
        ensureTPointersCapacity(builtTuples + tCount);
        for (int j = 0; j < tCount; ++j) {
            int ptr = builtTuples++;
            int tStart = inputTupleAccessor.getTupleStartOffset(j);
            int tEnd = inputTupleAccessor.getTupleEndOffset(j);
            tPointers[ptr * ptrSize + ID_FRAME_ID] = frameIndex;
            tPointers[ptr * ptrSize + ID_TUPLE_START] = tStart;
            tPointers[ptr * ptrSize + ID_TUPLE_END] = tEnd;
            if (nkcs == null) {
                continue;
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
            sortBucketSlice(currentBucketStart, len);
            // [Stage 3] a freshly sorted bucket is a level-0 run. Then, while the mergeFanIn newest runs are
            // all the same size, merge them into one run of the next level up
            pushRun(builtTuples, 0);
            while (numBuckets >= mergeFanIn && topRunsShareLevel(mergeFanIn)) {
                mergeTopRuns(mergeFanIn);
            }
            currentBucketStart = builtTuples;
            sortedFrameCount = getFrameCount(); // every frame so far is now in a sealed, sorted run
        }
        currentBucketBytes = 0;
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

    protected final int compare(int tp1, int tp2) throws HyracksDataException {
        return compare(tPointers, tp1, tPointers, tp2);
    }

    protected final int compare(int[] tPointers1, int tp1, int[] tPointers2, int tp2) throws HyracksDataException {
        if (nkcs != null) {
            int cmpNormalizedKey =
                    NormalizedKeyUtils.compareNormalizeKeys(tPointers1, tp1 * ptrSize + ID_NORMALIZED_KEY, tPointers2,
                            tp2 * ptrSize + ID_NORMALIZED_KEY, normalizedKeyTotalLength);
            if (cmpNormalizedKey != 0 || normalizedKeysDecisive) {
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
