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
import java.util.Random;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.buffermanager.EnumFreeSlotPolicy;
import org.apache.hyracks.dataflow.std.buffermanager.FrameFreeSlotPolicyFactory;
import org.apache.hyracks.dataflow.std.buffermanager.IFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.IFrameFreeSlotPolicy;
import org.apache.hyracks.dataflow.std.buffermanager.VariableFrameMemoryManager;
import org.apache.hyracks.dataflow.std.buffermanager.VariableFramePool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractExternalSortRunGenerator extends AbstractSortRunGenerator {

    protected final IHyracksTaskContext ctx;
    protected final IFrameSorter frameSorter;
    protected final int maxSortFrames;

    // *IMPORTANT*
    //
    // UPDATE: need to check if we can get memory BEFORE we flush. If we do get memory, should continue
    //          Or, could be victim. Need to find out here.
    //
    // GOAL:
    //  1. tunable parameter for probability of victim-ness. Use boolean flag. If victim, can't ask for more.
    //  2. if not victim, ask for more memory. broker might say yes, might say no. opposite probability to 1
    //  3. setup frequency of checking. Every x number of frames (~100), check victim-ness before inserting frame
    //      - what state is the system?
    //              - unused memory? currently sorting? used but not sorting? something else?
    private static final Logger ADAPT_LOGGER = LogManager.getLogger();
    private static final int ADAPT_CAP_MULTIPLIER = 4;
    private static final double VICTIM_PROBABILITY = 0.3; // tunable: P(broker victimizes us)
    private static final int VICTIM_CHECK_INTERVAL = 10; // poll the victim flag every N frames
    private final Random adaptiveRandom = new Random(0); // fixed seed --> reproducible experiment
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

        // [ADDED for memory-adaptive sort] original AsterixDB had none of these three lines.
        // Allow the budget to shrink to a safe floor and grow to a fixed ceiling; build the pool at
        // the ceiling (next line) so "grow" actually has frames to allocate.
        currentSortFrames = maxSortFrames;
        adaptiveMinFrames = Math.min(maxSortFrames, 16);
        adaptiveMaxFrames = maxSortFrames * ADAPT_CAP_MULTIPLIER;

        IFrameFreeSlotPolicy freeSlotPolicy = FrameFreeSlotPolicyFactory.createFreeSlotPolicy(policy, maxSortFrames);
        IFrameBufferManager bufferManager = new VariableFrameMemoryManager(
                // new VariableFramePool(ctx, maxSortFrames * ctx.getInitialFrameSize()), freeSlotPolicy);
                new VariableFramePool(ctx, adaptiveMaxFrames * ctx.getInitialFrameSize()), freeSlotPolicy);
        if (alg == Algorithm.MERGE_SORT) {
            frameSorter = new FrameSorterMergeSort(ctx, bufferManager, maxSortFrames, sortFields,
                    keyNormalizerFactories, comparatorFactories, recordDesc, outputLimit);
        } else {
            frameSorter = new FrameSorterQuickSort(ctx, bufferManager, maxSortFrames, sortFields,
                    keyNormalizerFactories, comparatorFactories, recordDesc, outputLimit);
        }
    }

    // ============================================================================================
    // ORIGINAL AsterixDB nextFrame (before our memory-adaptive changes) -- kept for the demo:
    //
    //   @Override
    //   public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
    //       if (!frameSorter.insertFrame(buffer)) {
    //           flushFramesToRun();
    //           if (!frameSorter.insertFrame(buffer)) {
    //               throw new HyracksDataException("The given frame is too big to insert into the sorting memory.");
    //           }
    //       }
    //   }
    //
    // It always spilled the moment the sorter filled up. The NEW version below instead polls a
    // simulated broker (periodic victim check) and, when full, asks for more memory before spilling.
    // ============================================================================================
    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        // (1) Periodic victim poll: every N frames, let the simulated broker reach us mid-run.
        //     If we are victimized, give up memory now: flush what we have and shrink the budget.
        if (++framesSeen % VICTIM_CHECK_INTERVAL == 0 && simulateVictim()) {
            long newBudgetBytes = (long) clampFrames(currentSortFrames / 2) * ctx.getInitialFrameSize();
            if (frameSorter.getUsedMemory() > newBudgetBytes) {
                flushFramesToRun();
                shrinkBudget("victim-periodic-spill");
            } else {
                shrinkBudget("victim-periodic-shrink");
            }
        }

        // (2) Try to place the frame. If we are full, ask the broker BEFORE spilling.
        if (!frameSorter.insertFrame(buffer)) {
            boolean victim = simulateVictim();
            if (!victim && currentSortFrames < adaptiveMaxFrames && simulateGrantMore()) {
                // not a victim and granted more: grow the CURRENT run and keep going, no spill
                growBudget("grant-grow");
            } else if (victim) {
                // victim: spill this run and shrink the budget for the next one
                flushFramesToRun();
                shrinkBudget("victim-full");
            } else {
                // denied (or already at the cap): spill, keep the same budget
                flushFramesToRun();
                logBudget("denied");
            }
            if (!frameSorter.insertFrame(buffer)) {
                throw new HyracksDataException("The given frame is too big to insert into the sorting memory.");
            }
        }
    }

    // ---- simulated broker + budget helpers (ADDED for memory-adaptive sort; no original equivalent) ----

    /** Simulated victim flag: the broker tells us to give memory back with this probability. */
    private boolean simulateVictim() {
        return adaptiveRandom.nextDouble() < VICTIM_PROBABILITY;
    }

    /** Simulated grant: when we ask for more, the broker agrees with the opposite probability. */
    private boolean simulateGrantMore() {
        return adaptiveRandom.nextDouble() < (1.0 - VICTIM_PROBABILITY);
    }

    private void growBudget(String reason) {
        setBudget(currentSortFrames * 2, reason);
    }

    private void shrinkBudget(String reason) {
        setBudget(currentSortFrames / 2, reason);
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
