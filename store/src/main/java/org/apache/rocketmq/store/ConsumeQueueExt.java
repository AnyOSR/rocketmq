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

package org.apache.rocketmq.store;

import org.apache.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Extend of consume queue, to store something not important,
 * such as message store time, filter bit map and etc.
 * <p/>
 * <li>1. This class is used only by {@link ConsumeQueue}</li>
 * <li>2. And is week reliable.</li>
 * <li>3. Be careful, address returned is always less than 0.</li>
 * <li>4. Pls keep this file small.</li>
 */
public class ConsumeQueueExt {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final MappedFileQueue mappedFileQueue;
    private final String topic;
    private final int queueId;

    private final String storePath;
    private final int mappedFileSize;
    private ByteBuffer tempContainer;           //bitMapLength(可能不到)bit位   不是线程安全

    public static final int END_BLANK_DATA_LENGTH = 4;

    /**
     * Addr can not exceed this value.For compatible.
     */
    //先将Integer.MIN_VALUE转换为Long 左端补最高位，大小刚好不变,源自IEEE754的特性
    public static final long MAX_ADDR = Integer.MIN_VALUE - 1L;                           //   0x FFFFFFFF 80000000 - 0x 00000000 00000001 = 0x FFFFFFFF 7FFFFFFF
    //一个long的负数，减去Long.MIN_VALUE，相当于减掉符号位
    //负数减掉一个Long.MIN_VALUE，相当于拿掉了符号位，变成了正数
    //正数加一个Long.MIN_VALUE，相当去加了一个符号位，变成了负数 一一映射，这是要干嘛？   这个值和mappedFileSize有关？
    public static final long MAX_REAL_OFFSET = MAX_ADDR - Long.MIN_VALUE;                 //   0x FFFFFFFF 7FFFFFFF - 0x 80000000 00000000 = 0x 7FFFFFFF 7FFFFFFF

    /**
     * Constructor.
     *
     * @param topic topic
     * @param queueId id of queue
     * @param storePath root dir of files to store.
     * @param mappedFileSize file size
     * @param bitMapLength bit map length.
     */
    public ConsumeQueueExt(final String topic, final int queueId, final String storePath, final int mappedFileSize, final int bitMapLength) {

        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;

        this.topic = topic;
        this.queueId = queueId;

        String queueDir = this.storePath + File.separator + topic + File.separator + queueId;

        this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);

        if (bitMapLength > 0) {
            this.tempContainer = ByteBuffer.allocate(bitMapLength / Byte.SIZE);
        }
    }

    /**
     * Check whether {@code address} point to extend file.
     * <p>
     * Just test {@code address} is less than 0.
     * </p>
     */
    //MAX_ADDR<0
    //返回true则表示address为负数，返回false，不能确定address的正负
    //ext Address 小于0
    public static boolean isExtAddr(final long address) {
        return address <= MAX_ADDR;
    }

    /**
     * Transform {@code address}(decorated by {@link #decorate}) to offset in mapped file.
     * <p>
     * if {@code address} is less than 0, return {@code address} - {@link java.lang.Long#MIN_VALUE};
     * else, just return {@code address}
     * </p>
     */
    //好奇怪？正address
    public long unDecorate(final long address) {
        if (isExtAddr(address)) {
            return address - Long.MIN_VALUE;
        }
        return address;
    }

    /**
     * Decorate {@code offset} from mapped file, in order to distinguish with tagsCode(saved in cq originally).
     * <p>
     * if {@code offset} is greater than or equal to 0, then return {@code offset} + {@link java.lang.Long#MIN_VALUE};
     * else, just return {@code offset}
     * </p>
     *
     * @return ext address(value is less than 0)  加上符号位
     */
    //返回负的address
    public long decorate(final long offset) {
        if (!isExtAddr(offset)) {
            return offset + Long.MIN_VALUE;
        }
        return offset;
    }

    /**
     * Get data from buffer.
     *
     * @param address less than 0
     */
    //统一编解码方式也行，但是为啥是MAX_ADDR这个值？
    //根据负的address，找到真实的offset，根据offset找到mapfile
    //读取mapfile slice出来的buffer来给cqExtUnit的数据结构赋值
    public CqExtUnit get(final long address) {
        CqExtUnit cqExtUnit = new CqExtUnit();
        if (get(address, cqExtUnit)) {
            return cqExtUnit;
        }

        return null;
    }

    /**
     * Get data from buffer, and set to {@code cqExtUnit}
     *
     * @param address less than 0
     */
    //根据address 构造cqExtUnit
    //返回构造是否成功
    public boolean get(final long address, final CqExtUnit cqExtUnit) {
        if (!isExtAddr(address)) {
            return false;
        }

        //是扩展地址
        final int mappedFileSize = this.mappedFileSize;
        final long realOffset = unDecorate(address);    //真实偏移

        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(realOffset, realOffset == 0);
        if (mappedFile == null) {
            return false;
        }

        int pos = (int) (realOffset % mappedFileSize);

        SelectMappedBufferResult bufferResult = mappedFile.selectMappedBuffer(pos);
        if (bufferResult == null) {
            log.warn("[BUG] Consume queue extend unit({}) is not found!", realOffset);
            return false;
        }
        boolean ret = false;
        try {
            ret = cqExtUnit.read(bufferResult.getByteBuffer());
        } finally {
            bufferResult.release();
        }

        return ret;
    }

    /**
     * Save to mapped buffer of file and return address.
     * <p>
     * Be careful, this method is not thread safe.
     * </p>
     *
     * @return success: < 0: fail: >=0
     */
    //成功返回的值小于0
    public long put(final CqExtUnit cqExtUnit) {
        final int retryTimes = 3;
        try {
            int size = cqExtUnit.calcUnitSize();
            //长度限制
            if (size > CqExtUnit.MAX_EXT_UNIT_SIZE) {
                log.error("Size of cq ext unit is greater than {}, {}", CqExtUnit.MAX_EXT_UNIT_SIZE, cqExtUnit);
                return 1;
            }
            //最大偏移量限制
            if (this.mappedFileQueue.getMaxOffset() + size > MAX_REAL_OFFSET) {
                log.warn("Capacity of ext is maximum!{}, {}", this.mappedFileQueue.getMaxOffset(), size);
                return 1;
            }
            // unit size maybe change.but, the same most of the time.
            if (this.tempContainer == null || this.tempContainer.capacity() < size) {
                this.tempContainer = ByteBuffer.allocate(size);
            }

            //重试
            for (int i = 0; i < retryTimes; i++) {
                MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

                if (mappedFile == null || mappedFile.isFull()) {
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                }

                if (mappedFile == null) {
                    log.error("Create mapped file when save consume queue extend, {}", cqExtUnit);
                    continue;
                }
                final int wrotePosition = mappedFile.getWrotePosition();
                //空闲容量
                final int blankSize = this.mappedFileSize - wrotePosition - END_BLANK_DATA_LENGTH;   //最后减掉四个是啥意思？

                // check whether has enough space.
                //如果空间不够 ，put一个short -1 然后最后将wrotePos设置为文件末尾
                if (size > blankSize) {
                    fullFillToEnd(mappedFile, wrotePosition);
                    log.info("No enough space(need:{}, has:{}) of file {}, so fill to end", size, blankSize, mappedFile.getFileName());
                    continue;
                }

                //返回扩展地址 是负的 小于0
                if (mappedFile.appendMessage(cqExtUnit.write(this.tempContainer), 0, size)) {
                    return decorate(wrotePosition + mappedFile.getFileFromOffset());
                }
            }
        } catch (Throwable e) {
            log.error("Save consume queue extend error, " + cqExtUnit, e);
        }

        return 1;
    }

    //-1 是结尾标志位？ 直接写到结尾
    protected void fullFillToEnd(final MappedFile mappedFile, final int wrotePosition) {
        ByteBuffer mappedFileBuffer = mappedFile.sliceByteBuffer();
        mappedFileBuffer.position(wrotePosition);

        // ending.
        mappedFileBuffer.putShort((short) -1);

        mappedFile.setWrotePosition(this.mappedFileSize);
    }

    /**
     * Load data from file when startup.
     */
    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load consume queue extend" + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
        return result;
    }

    /**
     * Check whether the step size in mapped file queue is correct.
     */
    public void checkSelf() {
        this.mappedFileQueue.checkSelf();
    }

    /**
     * Recover.
     */

    public void recover() {
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (mappedFiles == null || mappedFiles.isEmpty()) {
            return;
        }

        // load all files, consume queue will truncate extend files.
        int index = 0;

        MappedFile mappedFile = mappedFiles.get(index);
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
        long processOffset = mappedFile.getFileFromOffset();
        long mappedFileOffset = 0;
        CqExtUnit extUnit = new CqExtUnit();
        while (true) {
            extUnit.readBySkip(byteBuffer);                         //在当前mapfile内skip  能不从第一个mappedFile开始做？直接getLast

            // check whether write sth.
            if (extUnit.getSize() > 0) {
                mappedFileOffset += extUnit.getSize();               //当前已经处理到的位置
                continue;
            }

            index++;
            if (index < mappedFiles.size()) {
                mappedFile = mappedFiles.get(index);
                byteBuffer = mappedFile.sliceByteBuffer();
                processOffset = mappedFile.getFileFromOffset();
                mappedFileOffset = 0;
                log.info("Recover next consume queue extend file, " + mappedFile.getFileName());
                continue;
            }

            log.info("All files of consume queue extend has been recovered over, last mapped file " + mappedFile.getFileName());
            break;
        }

        processOffset += mappedFileOffset;
        this.mappedFileQueue.setFlushedWhere(processOffset);
        this.mappedFileQueue.setCommittedWhere(processOffset);
        this.mappedFileQueue.truncateDirtyFiles(processOffset);
    }

    /**
     * Delete files before {@code minAddress}.
     *
     * @param minAddress less than 0
     */
    //删除掉minAddress之前的file
    public void truncateByMinAddress(final long minAddress) {
        if (!isExtAddr(minAddress)) {
            return;
        }

        log.info("Truncate consume queue ext by min {}.", minAddress);

        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();

        List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        final long realOffset = unDecorate(minAddress);

        for (MappedFile file : mappedFiles) {
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;

            if (fileTailOffset < realOffset) {
                log.info("Destroy consume queue ext by min: file={}, fileTailOffset={}, minOffset={}", file.getFileName(), fileTailOffset, realOffset);
                if (file.destroy(1000)) {
                    willRemoveFiles.add(file);
                }
            }
        }

        this.mappedFileQueue.deleteExpiredFile(willRemoveFiles);
    }

    /**
     * Delete files after {@code maxAddress}, and reset wrote/commit/flush position to last file.
     *
     * @param maxAddress less than 0
     */
    //删除大于maxAddress的file
    public void truncateByMaxAddress(final long maxAddress) {
        if (!isExtAddr(maxAddress)) {
            return;
        }

        log.info("Truncate consume queue ext by max {}.", maxAddress);

        CqExtUnit cqExtUnit = get(maxAddress);
        if (cqExtUnit == null) {
            log.error("[BUG] address {} of consume queue extend not found!", maxAddress);
            return;
        }

        final long realOffset = unDecorate(maxAddress);

        this.mappedFileQueue.truncateDirtyFiles(realOffset + cqExtUnit.getSize());
    }

    /**
     * flush buffer to file.
     */
    public boolean flush(final int flushLeastPages) {
        return this.mappedFileQueue.flush(flushLeastPages);
    }

    /**
     * delete files and directory.
     */
    public void destroy() {
        this.mappedFileQueue.destroy();
    }

    /**
     * Max address(value is less than 0).
     * <p/>
     * <p>
     * Be careful: it's an address just when invoking this method.
     * </p>
     */
    public long getMaxAddress() {
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (mappedFile == null) {
            return decorate(0);
        }
        return decorate(mappedFile.getFileFromOffset() + mappedFile.getWrotePosition());
    }

    /**
     * Minus address saved in file.
     */
    public long getMinAddress() {
        MappedFile firstFile = this.mappedFileQueue.getFirstMappedFile();
        if (firstFile == null) {
            return decorate(0);
        }
        return decorate(firstFile.getFileFromOffset());                    //干嘛非得decorate？
    }

    /**
     * Store unit.
     */
    public static class CqExtUnit {

        public static final short MIN_EXT_UNIT_SIZE
            = 2 * 1 // size, 32k max
            + 8 * 2 // msg time + tagCode
            + 2; // bitMapSize

        public static final int MAX_EXT_UNIT_SIZE = Short.MAX_VALUE;  // 32768 b/ 1kb = 32    32k

        public CqExtUnit() {
        }

        //每个消息体的大小为20 + bitMapSize
        public CqExtUnit(Long tagsCode, long msgStoreTime, byte[] filterBitMap) {
            this.tagsCode = tagsCode == null ? 0 : tagsCode;
            this.msgStoreTime = msgStoreTime;
            this.filterBitMap = filterBitMap;
            this.bitMapSize = (short) (filterBitMap == null ? 0 : filterBitMap.length);
            this.size = (short) (MIN_EXT_UNIT_SIZE + this.bitMapSize);               //size = 20+ filterBitMap.length >=20
        }

        /**
         * unit size
         */
        private short size;
        /**
         * has code of tags
         */
        private long tagsCode;
        /**
         * the time to store into commit log of message
         */
        private long msgStoreTime;
        /**
         * size of bit map
         */
        private short bitMapSize;
        /**
         * filter bit map           filterBitMap.length = bitMapSize
         */
        private byte[] filterBitMap;

        /**
         * build unit from buffer from current position.
         */
        //从buffer中构造一个CqExtUnit
        private boolean read(final ByteBuffer buffer) {
            // buffer.position()==buffer.limit()-1  至多只有一个字节可读了
            if (buffer.position() + 2 > buffer.limit()) {
                return false;
            }

            //buffer.position() + 2 <= buffer.limit() 至少有两个字节可读
            this.size = buffer.getShort();

            if (this.size < 1) {
                return false;
            }

            //size(2) + tagsCode(8) + msgStoreTime(8) + bitMapSize(2)
            this.tagsCode = buffer.getLong();
            this.msgStoreTime = buffer.getLong();
            this.bitMapSize = buffer.getShort();

            if (this.bitMapSize < 1) {
                return true;
            }

            //这里不用校验size和bitMapSize的关系吗？
            if (this.filterBitMap == null || this.filterBitMap.length != this.bitMapSize) {
                this.filterBitMap = new byte[bitMapSize];
            }

            //读入bitMapSize字节到filterBitMap
            buffer.get(this.filterBitMap);
            return true;
        }

        /**
         * Only read first 2 byte to get unit size.
         * <p>
         * if size > 0, then skip buffer position with size.
         * </p>
         * <p>
         * if size <= 0, nothing to do.
         * </p>
         */
        //直接跳到下一CqExtUnit
        //改变了this.size
        private void readBySkip(final ByteBuffer buffer) {
            ByteBuffer temp = buffer.slice();

            short tempSize = temp.getShort();
            this.size = tempSize;

            if (tempSize > 0) {
                buffer.position(buffer.position() + this.size);
            }
        }

        /**
         * Transform unit data to byte array.
         * <p/>
         * <li>1. @{code container} can be null, it will be created if null.</li>
         * <li>2. if capacity of @{code container} is less than unit size, it will be created also.</li>
         * <li>3. Pls be sure that size of unit is not greater than {@link #MAX_EXT_UNIT_SIZE}</li>
         */
        //将this(CqExtUnit)转换为一个byte数组
        //如果container的大小大于this.size,container的内容将被改变
        private byte[] write(final ByteBuffer container) {
            this.bitMapSize = (short) (filterBitMap == null ? 0 : filterBitMap.length);
            this.size = (short) (MIN_EXT_UNIT_SIZE + this.bitMapSize);

            ByteBuffer temp = container;

            if (temp == null || temp.capacity() < this.size) {
                temp = ByteBuffer.allocate(this.size);
            }

            temp.flip();
            temp.limit(this.size);

            //这里没有检查size的大小
            temp.putShort(this.size);
            temp.putLong(this.tagsCode);
            temp.putLong(this.msgStoreTime);
            temp.putShort(this.bitMapSize);
            if (this.bitMapSize > 0) {
                temp.put(this.filterBitMap);
            }

            return temp.array();
        }

        /**
         * Calculate unit size by current data.
         */
        //计算当前CqExtUnit的size
        private int calcUnitSize() {
            int sizeTemp = MIN_EXT_UNIT_SIZE + (filterBitMap == null ? 0 : filterBitMap.length);
            return sizeTemp;
        }

        public long getTagsCode() {
            return tagsCode;
        }

        public void setTagsCode(final long tagsCode) {
            this.tagsCode = tagsCode;
        }

        public long getMsgStoreTime() {
            return msgStoreTime;
        }

        public void setMsgStoreTime(final long msgStoreTime) {
            this.msgStoreTime = msgStoreTime;
        }

        public byte[] getFilterBitMap() {
            if (this.bitMapSize < 1) {
                return null;
            }
            return filterBitMap;
        }

        public void setFilterBitMap(final byte[] filterBitMap) {
            this.filterBitMap = filterBitMap;
            // not safe transform, but size will be calculate by #calcUnitSize
            this.bitMapSize = (short) (filterBitMap == null ? 0 : filterBitMap.length);
        }

        public short getSize() {
            return size;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof CqExtUnit))
                return false;

            CqExtUnit cqExtUnit = (CqExtUnit) o;

            if (bitMapSize != cqExtUnit.bitMapSize)
                return false;
            if (msgStoreTime != cqExtUnit.msgStoreTime)
                return false;
            if (size != cqExtUnit.size)
                return false;
            if (tagsCode != cqExtUnit.tagsCode)
                return false;
            if (!Arrays.equals(filterBitMap, cqExtUnit.filterBitMap))
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) size;
            result = 31 * result + (int) (tagsCode ^ (tagsCode >>> 32));
            result = 31 * result + (int) (msgStoreTime ^ (msgStoreTime >>> 32));
            result = 31 * result + (int) bitMapSize;
            result = 31 * result + (filterBitMap != null ? Arrays.hashCode(filterBitMap) : 0);
            return result;
        }

        @Override
        public String toString() {
            return "CqExtUnit{" +
                "size=" + size +
                ", tagsCode=" + tagsCode +
                ", msgStoreTime=" + msgStoreTime +
                ", bitMapSize=" + bitMapSize +
                ", filterBitMap=" + Arrays.toString(filterBitMap) +
                '}';
        }
    }
}
