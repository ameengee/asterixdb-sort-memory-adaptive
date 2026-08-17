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

import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IFrameSorter extends ISorter {

    int getFrameCount();

    boolean insertFrame(ByteBuffer inputBuffer) throws HyracksDataException;

    // [ADDED for memory-adaptive sort]
    // Allow the sort's memory budget to be changed between runs; only the sorter's own budget gate is affected.
    void setMaxSortMemory(long maxSortMemory);

    // [ADDED for memory-adaptive sort] current in-memory bytes held by this run (frame bytes +
    // pointer reservations). Lets the run generator decide whether a shrink actually requires a spill.
    long getUsedMemory();

    // [ADDED for memory-adaptive sort] total tuples loaded, and how many are already sorted (in sealed buckets).
    int getTupleCount();

    int getSortedTupleCount();

}
