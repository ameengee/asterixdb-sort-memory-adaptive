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

    /**
     * All numeric types share one ordering class, because AsterixDB compares numerics by promoted
     * value rather than by tag. The value 4 (BIGINT's raw tag) is chosen so the merged class still
     * sits correctly relative to the non-numeric tags that matter in practice -- STRING(13),
     * BOOLEAN(15), the date/time family(16-18, 36-37), UUID(38). The tags that fall between the
     * numeric ranges (UINT8-64 = 5-8, BINARY = 9, BITARRAY = 10) cannot be ordered correctly against
     * it; those mark the key invalid instead.
     */
    private static final int NUMERIC_CLASS = 4;

    /** Order-preserving 64-bit image of a double: flip the sign bit, or invert if negative. */
    private static long doubleBits(double d) {
        long b = Double.doubleToLongBits(d);
        return b >= 0 ? (b ^ Long.MIN_VALUE) : ~b;
    }

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
            private boolean valid = true;

            @Override
            public void normalize(byte[] bytes, int start, int length, int[] keys, int keyStart) {
                byte tag = bytes[start];
                ATypeTag t = ATypeTag.VALUE_TYPE_MAPPING[tag];
                int vs = start + 1; // skip the Asterix type tag byte
                long v; // order-preserving 64-bit image of the value, within its class
                int cls; // ordering class -- see below
                switch (t == null ? ATypeTag.ANY : t) {
                    // ---- NUMERIC FAMILY -------------------------------------------------------
                    // The comparator compares numerics by PROMOTED VALUE and ignores their tags, so
                    // every numeric type must share one class and encode a comparable image. Double
                    // promotion is monotonic: distinct values may collide (ties fall through to the
                    // comparator) but never invert.
                    case TINYINT:
                        v = doubleBits(bytes[vs]);
                        cls = NUMERIC_CLASS;
                        break;
                    case SMALLINT:
                        v = doubleBits((short) ((bytes[vs] << 8) | (bytes[vs + 1] & 0xff)));
                        cls = NUMERIC_CLASS;
                        break;
                    case INTEGER:
                        v = doubleBits(IntegerPointable.getInteger(bytes, vs));
                        cls = NUMERIC_CLASS;
                        break;
                    case BIGINT:
                        v = doubleBits(LongPointable.getLong(bytes, vs));
                        cls = NUMERIC_CLASS;
                        break;
                    case FLOAT:
                        v = doubleBits(Float.intBitsToFloat(IntegerPointable.getInteger(bytes, vs)));
                        cls = NUMERIC_CLASS;
                        break;
                    case DOUBLE:
                        v = doubleBits(Double.longBitsToDouble(LongPointable.getLong(bytes, vs)));
                        cls = NUMERIC_CLASS;
                        break;
                    // ---- NON-NUMERIC: ordered against each other by RAW TAG BYTE ---------------
                    case STRING:
                        v = (((long) UTF8StringUtil.normalize(bytes, vs)) & 0xffffffffL) << 24;
                        cls = tag & 0xff;
                        break;
                    case BOOLEAN:
                        v = bytes[vs] != 0 ? 1L : 0L;
                        cls = tag & 0xff;
                        break;
                    case DATE:
                    case TIME:
                    case YEARMONTHDURATION:
                        v = ((long) IntegerPointable.getInteger(bytes, vs)) ^ 0x80000000L;
                        cls = tag & 0xff;
                        break;
                    case DATETIME:
                    case DAYTIMEDURATION:
                        v = LongPointable.getLong(bytes, vs) ^ Long.MIN_VALUE;
                        cls = tag & 0xff;
                        break;
                    case MISSING:
                        v = 0;
                        cls = 0; // MISSING sorts before everything
                        break;
                    case NULL:
                        v = 0;
                        cls = 1; // NULL sorts after MISSING, before everything else
                        break;
                    default:
                        // Types whose RAW tag byte falls between the numeric tags (UINT8-64 = 5-8,
                        // BINARY = 9, BITARRAY = 10) cannot be ordered correctly against the merged
                        // numeric class, and unknown tags cannot be encoded at all. Refuse rather
                        // than risk a wrong ordering.
                        valid = false;
                        v = 0;
                        cls = tag & 0xff;
                        break;
                }
                // class in the top 8 bits, value image below it
                long key = (((long) cls) << 56) | ((v >>> 8) & 0x00ffffffffffffffL);
                if (!asc) {
                    key = ~key;
                }
                if (w == 1) {
                    keys[keyStart] = (int) (key >>> 32);
                } else {
                    keys[keyStart] = (int) (key >>> 32);
                    keys[keyStart + 1] = (int) key;
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
