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
package org.apache.asterix.dataflow.data.nontagged.keynormalizers;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyProperties;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.util.string.UTF8StringUtil;

/**
 * A normalized key computer that infers the column's type AT RUNTIME from the value's type tag.
 * <p>
 * <b>Why this exists.</b> {@code NormalizedKeyComputerFactoryProvider} returns {@code null} when the
 * sort key's static type is unknown -- which is the case for any UNDECLARED field of an {@code open}
 * type, since its type is {@code ANY}. The sorter then has no normalized key at all and every
 * comparison runs the full binary comparator, dereferencing tuple bytes. Measured on 10M rows, that
 * costs 3-4x per comparison (47-59ns vs 12-18ns) and roughly 2x end to end.
 * <p>
 * <b>Why it must detect homogeneity.</b> AsterixDB orders values of different types by comparing
 * their type tags, EXCEPT that numeric types are compared by promoted value. Those two rules cannot
 * both be encoded in a fixed-width key: the numeric tags (1-4, 11-12) are not contiguous, so no
 * single "numeric" class byte can sit correctly relative to BINARY(9), BITARRAY(10) or the UINTs
 * (5-8) that fall between them. A key that encodes the tag would order {@code INTEGER 100} before
 * {@code DOUBLE 5.0}, which is wrong.
 * <p>
 * So this computer is only sound while the column is homogeneous. It records the first tag it sees
 * and, on any later value with a different tag, reports {@link #isKeyValid()} == false. The caller
 * MUST then stop using normalized keys and re-derive ordering from the comparator alone.
 * <p>
 * Because homogeneity is guaranteed while the key is valid, no bits are spent on the tag -- the full
 * width encodes the value. Width is configurable: one int is fully discriminating for values inside
 * 32 bits and ties only ~1 in 4 billion otherwise, while costing one less int per tuple in the
 * sorter's pointer array.
 * <p>
 * Never decisive: ties always fall through to the real comparator.
 */
public class DynamicNormalizedKeyComputerFactory implements INormalizedKeyComputerFactory {

    private static final long serialVersionUID = 1L;
    /** ints per key; 1 or 2. */
    private final int width;
    private final boolean ascending;

    public DynamicNormalizedKeyComputerFactory(int width, boolean ascending) {
        this.width = width < 2 ? 1 : 2;
        this.ascending = ascending;
    }

    private INormalizedKeyProperties properties() {
        final int w = width;
        return new INormalizedKeyProperties() {
            private static final long serialVersionUID = 1L;

            @Override
            public int getNormalizedKeyLength() {
                return w;
            }

            @Override
            public boolean isDecisive() {
                // never decisive: the comparator must arbitrate ties, and must arbitrate everything
                // once the column turns out to be heterogeneous
                return false;
            }
        };
    }

    @Override
    public INormalizedKeyProperties getNormalizedKeyProperties() {
        return properties();
    }

    @Override
    public INormalizedKeyComputer createNormalizedKeyComputer() {
        final INormalizedKeyProperties props = properties();
        final int w = width;
        final boolean asc = ascending;
        return new INormalizedKeyComputer() {
            private byte firstTag = -1;
            private boolean valid = true;

            @Override
            public void normalize(byte[] bytes, int start, int length, int[] keys, int keyStart) {
                byte tag = bytes[start];
                if (firstTag < 0) {
                    firstTag = tag;
                } else if (tag != firstTag) {
                    valid = false;
                }
                long v;
                int vs = start + 1; // skip the Asterix type tag byte
                switch (ATypeTag.VALUE_TYPE_MAPPING[tag] == null ? ATypeTag.ANY : ATypeTag.VALUE_TYPE_MAPPING[tag]) {
                    case TINYINT:
                        v = ((long) bytes[vs]) ^ Long.MIN_VALUE;
                        break;
                    case SMALLINT:
                        v = ((long) ((short) ((bytes[vs] << 8) | (bytes[vs + 1] & 0xff)))) ^ Long.MIN_VALUE;
                        break;
                    case INTEGER:
                    case DATE:
                    case TIME:
                    case YEARMONTHDURATION:
                        v = ((long) IntegerPointable.getInteger(bytes, vs)) ^ Long.MIN_VALUE;
                        break;
                    case BIGINT:
                    case DATETIME:
                    case DAYTIMEDURATION:
                        v = LongPointable.getLong(bytes, vs) ^ Long.MIN_VALUE;
                        break;
                    case FLOAT: {
                        int f = IntegerPointable.getInteger(bytes, vs);
                        // order-preserving transform for IEEE floats
                        f = f >= 0 ? (f ^ Integer.MIN_VALUE) : ~f;
                        v = ((long) f) << 32;
                        break;
                    }
                    case DOUBLE: {
                        long d = LongPointable.getLong(bytes, vs);
                        d = d >= 0 ? (d ^ Long.MIN_VALUE) : ~d;
                        v = d;
                        break;
                    }
                    case STRING: {
                        int p = UTF8StringUtil.normalize(bytes, vs);
                        v = (((long) p) & 0xffffffffL) << 32;
                        break;
                    }
                    case BOOLEAN:
                        v = (bytes[vs] != 0 ? 1L : 0L) ^ Long.MIN_VALUE;
                        break;
                    default:
                        // unknown/unsupported tag: emit a constant so every such value ties and the
                        // comparator decides. Combined with the homogeneity check this stays sound.
                        v = 0L;
                        break;
                }
                if (!asc) {
                    v = ~v;
                }
                if (w == 1) {
                    keys[keyStart] = (int) (v >>> 32);
                } else {
                    keys[keyStart] = (int) (v >>> 32);
                    keys[keyStart + 1] = (int) v;
                }
            }

            @Override
            public INormalizedKeyProperties getNormalizedKeyProperties() {
                return props;
            }

            @Override
            public boolean isKeyValid() {
                return valid;
            }
        };
    }
}
