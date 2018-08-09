/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.filter.util;

import java.util.Arrays;

/**
 * Data generated by bloom filter, include:
 * <li>1. Bit positions allocated to requester;</li>
 * <li>2. Total bit num when allocating;</li>
 */
//BitsArray里面bit的位置信息
//BloomFilterData保存了位置信息
public class BloomFilterData {

    private int[] bitPos;
    private int bitNum;

    public BloomFilterData() {
    }

    public BloomFilterData(int[] bitPos, int bitNum) {
        this.bitPos = bitPos;
        this.bitNum = bitNum;
    }

    public int[] getBitPos() {
        return bitPos;
    }

    public int getBitNum() {
        return bitNum;
    }

    public void setBitPos(final int[] bitPos) {
        this.bitPos = bitPos;
    }

    public void setBitNum(final int bitNum) {
        this.bitNum = bitNum;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (!(o instanceof BloomFilterData))
            return false;

        final BloomFilterData that = (BloomFilterData) o;

        if (bitNum != that.bitNum)
            return false;
        if (!Arrays.equals(bitPos, that.bitPos))
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = bitPos != null ? Arrays.hashCode(bitPos) : 0;
        result = 31 * result + bitNum;
        return result;
    }

    @Override
    public String toString() {
        return "BloomFilterData{" +
            "bitPos=" + Arrays.toString(bitPos) +
            ", bitNum=" + bitNum +
            '}';
    }
}
