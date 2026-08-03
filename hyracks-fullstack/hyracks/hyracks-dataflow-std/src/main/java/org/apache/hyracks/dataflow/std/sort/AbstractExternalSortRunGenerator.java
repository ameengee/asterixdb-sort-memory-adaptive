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

    // ---- DUMMY memory-adaptive sort (experiment) ----
    // At each run boundary we randomly keep / halve / double the in-memory sort budget for the
    // NEXT run. The frame pool is sized to ADAPT_CAP_MULTIPLIER x the nominal budget so that
    // "double" has real headroom
    private static final Logger ADAPT_LOGGER = LogManager.getLogger();
    private static final int ADAPT_CAP_MULTIPLIER = 4;
    private final Random adaptiveRandom = new Random(0); // fixed seed --> reproducible experiment
    private final int adaptiveMinFrames; // floor, kept safely above single-frame needs
    private final int adaptiveMaxFrames; // ceiling == pool capacity in frames
    private int currentSortFrames; // budget (in frames) of the run currently being built

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

        // DUMMY adaptive sort: allow memory budger to shrink down to a safe floor and growth up to a fixed ceiling.
        // Build the pool at the ceiling so "double" actually has frames to allocate
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

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        if (!frameSorter.insertFrame(buffer)) {
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
            flushFramesToRun();
            adjustBudgetForNextRun(); // DUMMY adaptive sort: choose the next run's memory budget
            if (!frameSorter.insertFrame(buffer)) {
                throw new HyracksDataException("The given frame is too big to insert into the sorting memory.");
            }
        }
    }

    /**
     * DUMMY memory-adaptive sort (experiment). Invoked at each run boundary, immediately after a
     * run has been sorted, spilled to disk, and the sorter reset. Randomly keeps, halves, or
     * doubles the in-memory sort budget for the next run, clamped to
     * [adaptiveMinFrames, adaptiveMaxFrames]. Only the sorter's own budget gate is changed.
     */
    private void adjustBudgetForNextRun() {
        int roll = adaptiveRandom.nextInt(3) + 1; // 1, 2, or 3
        int next;
        switch (roll) {
            case 2:
                next = currentSortFrames / 2; // halve
                break;
            case 3:
                next = currentSortFrames * 2; // double
                break;
            default:
                next = currentSortFrames; // keep
                break;
        }
        next = Math.max(adaptiveMinFrames, Math.min(adaptiveMaxFrames, next));
        currentSortFrames = next;
        frameSorter.setMaxSortMemory((long) currentSortFrames * ctx.getInitialFrameSize());
        if (ADAPT_LOGGER.isInfoEnabled()) {
            ADAPT_LOGGER.info("adaptive-sort: roll={} nextRunFrames={} (min={}, max={})", roll, currentSortFrames,
                    adaptiveMinFrames, adaptiveMaxFrames);
        }
    }

    @Override
    public ISorter getSorter() {
        return frameSorter;
    }

}
