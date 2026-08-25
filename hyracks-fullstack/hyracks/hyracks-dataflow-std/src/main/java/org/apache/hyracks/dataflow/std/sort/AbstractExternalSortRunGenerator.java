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

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.AdaptiveVariableFrameMemoryManager;
import org.apache.hyracks.dataflow.std.buffermanager.EnumFreeSlotPolicy;
import org.apache.hyracks.dataflow.std.buffermanager.FrameFreeSlotPolicyFactory;
import org.apache.hyracks.dataflow.std.buffermanager.IBrokerConduit;
import org.apache.hyracks.dataflow.std.buffermanager.IFrameFreeSlotPolicy;
import org.apache.hyracks.dataflow.std.buffermanager.MemoryStatus;
import org.apache.hyracks.dataflow.std.buffermanager.RandomMemoryBroker;
import org.apache.hyracks.dataflow.std.buffermanager.VariableFramePool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractExternalSortRunGenerator extends AbstractSortRunGenerator {

    protected final IHyracksTaskContext ctx;
    protected final IFrameSorter frameSorter;
    protected final int maxSortFrames;

    // [ADDED for memory-adaptive sort]
    private static final Logger ADAPT_LOGGER = LogManager.getLogger();
    private static final int ADAPT_CAP_MULTIPLIER = 4;
    private static final double VICTIM_PROBABILITY = 0.3; // tunable: P(broker victimizes us)
    private static final int VICTIM_CHECK_INTERVAL = 10; // poll the broker every N frames
    private final IBrokerConduit broker; // == the adaptive buffer manager (relays status to the broker)
    private final int adaptiveMinFrames; // floor, kept safely above single-frame needs
    private final int adaptiveMaxFrames; // ceiling == pool capacity in frames
    private int currentSortFrames; // budget (in frames) of the run currently being built
    private int framesSeen; // counts frames for the periodic victim poll

    public AbstractExternalSortRunGenerator(IHyracksTaskContext ctx, int[] sortFields,
            INormalizedKeyComputerFactory[] keyNormalizerFactories, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDesc, Algorithm alg, int framesLimit) throws HyracksDataException {
        this(ctx, sortFields, keyNormalizerFactories, comparatorFactories, recordDesc, alg, EnumFreeSlotPolicy.LAST_FIT,
                framesLimit);
    }

    public AbstractExternalSortRunGenerator(IHyracksTaskContext ctx, int[] sortFields,
            INormalizedKeyComputerFactory[] keyNormalizerFactories, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDesc, Algorithm alg, EnumFreeSlotPolicy policy, int framesLimit)
            throws HyracksDataException {
        this(ctx, sortFields, keyNormalizerFactories, comparatorFactories, recordDesc, alg, policy, framesLimit,
                Integer.MAX_VALUE);
    }

    public AbstractExternalSortRunGenerator(IHyracksTaskContext ctx, int[] sortFields,
            INormalizedKeyComputerFactory[] keyNormalizerFactories, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDesc, Algorithm alg, EnumFreeSlotPolicy policy, int framesLimit, int outputLimit)
            throws HyracksDataException {
        super();
        this.ctx = ctx;
        maxSortFrames = framesLimit - 1;

        // [ADDED for memory-adaptive sort]
        // Allow the budget to shrink to a safe floor and grow to a fixed ceiling.
        // build the pool at ceiling so "grow" actually has frames to allocate.
        currentSortFrames = maxSortFrames;
        adaptiveMinFrames = Math.min(maxSortFrames, 16);
        adaptiveMaxFrames = maxSortFrames * ADAPT_CAP_MULTIPLIER;

        IFrameFreeSlotPolicy freeSlotPolicy = FrameFreeSlotPolicyFactory.createFreeSlotPolicy(policy, maxSortFrames);
        AdaptiveVariableFrameMemoryManager bufferManager = new AdaptiveVariableFrameMemoryManager(
                new VariableFramePool(ctx, adaptiveMaxFrames * ctx.getInitialFrameSize()), freeSlotPolicy,
                new RandomMemoryBroker(VICTIM_PROBABILITY, 0));
        this.broker = bufferManager;
        if (alg == Algorithm.MERGE_SORT) {
            frameSorter = new FrameSorterMergeSort(ctx, bufferManager, maxSortFrames, sortFields,
                    keyNormalizerFactories, comparatorFactories, recordDesc, outputLimit);
        } else {
            frameSorter = new FrameSorterQuickSort(ctx, bufferManager, maxSortFrames, sortFields,
                    keyNormalizerFactories, comparatorFactories, recordDesc, outputLimit);
        }
    }

    // IMPORTANT: set spilled frames to null, need to allow GC to reclaim that memory. Cannot lie about reducing
    // budget and call it a day.
    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        // (1) Every N frames: send current status to broker and read victim flag. If victim, shrink budget
        if (++framesSeen % VICTIM_CHECK_INTERVAL == 0) {
            broker.reportStatus(buildStatus()); // fire-and-forget
            long reclaim = broker.getReclaimDemand(); // local, non-blocking read: 0 or -N
            if (reclaim < 0) {
                int newFrames = clampFrames(currentSortFrames + (int) reclaim);
                long newBudgetBytes = (long) newFrames * ctx.getInitialFrameSize();
                if (frameSorter.getUsedMemory() > newBudgetBytes) {
                    // [Stage 2] give back memory the cheap way: spill the sorted part, keep the unsorted tail
                    spillSortedKeepUnsortedToRun();
                    setBudget(newFrames, "victim-periodic-spill");
                } else {
                    setBudget(newFrames, "victim-periodic-shrink");
                }
            }
        }

        // (2) Try to place the frame. If we are full, ask the broker (synchronously) for more BEFORE spilling.
        if (!frameSorter.insertFrame(buffer)) {
            long delta = broker.requestMore(buildStatus());
            if (delta > 0 && currentSortFrames < adaptiveMaxFrames) {
                setBudget(currentSortFrames + (int) delta, "grant-grow");
            } else if (delta < 0) {
                // [Stage 2] victim while full: spill the sorted part, keep the unsorted tail
                spillSortedKeepUnsortedToRun();
                setBudget(currentSortFrames + (int) delta, "victim-full");
            } else {
                flushFramesToRun();
                logBudget("denied");
            }
            if (!frameSorter.insertFrame(buffer)) {
                throw new HyracksDataException("The given frame is too big to insert into the sorting memory.");
            }
        }
    }

    // [ADDED for memory-adaptive sort] Build the 3-tier memory status the operator hands to the broker.
    // easy = unused budget. General slowdown, no damage.
    // medium = loaded AND already sorted. No repeat work, but general slowdown + shorter run.
    // hard = loaded but not yet sorted. Repeat I/O work, general slowdown, and shorter run
    private MemoryStatus buildStatus() {
        int loadedFrames = frameSorter.getFrameCount();
        int totalTuples = frameSorter.getTupleCount();
        int sortedTuples = frameSorter.getSortedTupleCount();
        long easy = Math.max(0, currentSortFrames - loadedFrames);
        long medium = totalTuples > 0 ? (long) loadedFrames * sortedTuples / totalTuples : loadedFrames;
        long hard = loadedFrames - medium;
        return new MemoryStatus(easy, medium, hard);
    }

    private int clampFrames(int requestedFrames) {
        return Math.max(adaptiveMinFrames, Math.min(adaptiveMaxFrames, requestedFrames));
    }

    private void setBudget(int requestedFrames, String reason) {
        currentSortFrames = clampFrames(requestedFrames);
        frameSorter.setMaxSortMemory((long) currentSortFrames * ctx.getInitialFrameSize());
        logBudget(reason);
    }

    private void logBudget(String reason) {
        if (ADAPT_LOGGER.isInfoEnabled()) {
            ADAPT_LOGGER.info("adaptive-sort: {} budgetFrames={} (min={}, max={})", reason, currentSortFrames,
                    adaptiveMinFrames, adaptiveMaxFrames);
        }
    }

    @Override
    public ISorter getSorter() {
        return frameSorter;
    }

}
