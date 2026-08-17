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
package org.apache.hyracks.dataflow.std.buffermanager;

import org.apache.hyracks.api.context.IHyracksFrameMgrContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * [Memory-adaptive sort] {@link VPartitionTupleBufferManager} + a broker relay, for the operators that
 * use the partitioned tuple buffer manager (hash join, sort-group-by, etc.). Adds nothing to the
 * buffer-management behavior; it only holds an {@link IMemoryBroker} and forwards the operator's
 * {@link MemoryStatus} to it (see {@link IBrokerConduit}). It does NOT spill/release memory itself.
 * <p>
 * Not yet wired to any operator -- it exists so those operators can adopt the same single-broker-contact
 * pattern the sort now uses.
 */
public class AdaptiveVPartitionTupleBufferManager extends VPartitionTupleBufferManager implements IBrokerConduit {

    private final IMemoryBroker broker;
    private long reclaimDemand; // broker-set flag: 0 = not a victim, -N = give N frames back

    public AdaptiveVPartitionTupleBufferManager(IPartitionedMemoryConstrain constrain, int partitions,
            IDeallocatableFramePool framePool, IMemoryBroker broker) throws HyracksDataException {
        super(constrain, partitions, framePool);
        this.broker = broker;
    }

    public AdaptiveVPartitionTupleBufferManager(IHyracksFrameMgrContext ctx, IPartitionedMemoryConstrain constrain,
            int partitions, int frameLimitInBytes, IMemoryBroker broker) throws HyracksDataException {
        super(ctx, constrain, partitions, frameLimitInBytes);
        this.broker = broker;
    }

    @Override
    public void reportStatus(MemoryStatus status) {
        // fire-and-forget from the operator's view; the (random) broker sets our reclaim demand here.
        reclaimDemand = broker.onStatusUpdate(status);
    }

    @Override
    public long getReclaimDemand() {
        return reclaimDemand; // local, non-blocking read of the broker-set flag
    }

    @Override
    public long requestMore(MemoryStatus status) {
        return broker.requestMore(status); // synchronous
    }
}
