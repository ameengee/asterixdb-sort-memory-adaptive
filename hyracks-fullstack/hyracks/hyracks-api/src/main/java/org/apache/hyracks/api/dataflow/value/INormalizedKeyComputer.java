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
package org.apache.hyracks.api.dataflow.value;

public interface INormalizedKeyComputer {
    void normalize(byte[] bytes, int start, int length, int[] normalizedKeys, int keyStart);

    INormalizedKeyProperties getNormalizedKeyProperties();

    /**
     * Whether the keys produced so far are still usable for ordering.
     * <p>
     * Static normalizers always return {@code true}: the column's type is known at compile time, so
     * every key is computed the same way. A RUNTIME-TYPE-DETECTING normalizer cannot promise that.
     * It infers the type from the first value it sees, which is only sound while the column stays
     * homogeneous -- AsterixDB compares values of different types by comparing their type tags
     * (and numeric types by promoted value), and no fixed-width key can reproduce both rules at
     * once. On seeing a second distinct type tag such a normalizer returns {@code false}, and the
     * caller must stop using normalized keys and fall back to the binary comparator, discarding any
     * ordering already derived from them.
     */
    default boolean isKeyValid() {
        return true;
    }
}
