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
package org.apache.rocketmq.common.sysflag;

//低2,3位为transaction标志相关
public class MessageSysFlag {
    public final static int TRANSACTION_NOT_TYPE = 0;               // 00000000 00000000 00000000 00000000
    public final static int COMPRESSED_FLAG = 0x1;                  // 00000000 00000000 00000000 00000001
    public final static int MULTI_TAGS_FLAG = 0x1 << 1;             // 00000000 00000000 00000000 00000010
    public final static int TRANSACTION_PREPARED_TYPE = 0x1 << 2;   // 00000000 00000000 00000000 00000100
    public final static int TRANSACTION_COMMIT_TYPE = 0x2 << 2;     // 00000000 00000000 00000000 00001000
    public final static int TRANSACTION_ROLLBACK_TYPE = 0x3 << 2;   // 00000000 00000000 00000000 00001100

    public static int getTransactionValue(final int flag) {
        return flag & TRANSACTION_ROLLBACK_TYPE;
    }

    //首先将flag的低2,3位置为0， | 上type并返回
    //就是先将flag的TRANSACTION_ROLLBACK_TYPE位清0，然后将type的TRANSACTION_ROLLBACK_TYPE值赋给flag
    //根据type的TRANSACTION_ROLLBACK_TYPE值来设定返回的值的TRANSACTION_ROLLBACK_TYPE
    public static int resetTransactionValue(final int flag, final int type) {
        return (flag & (~TRANSACTION_ROLLBACK_TYPE)) | type;
    }

    //将flag的最后一位置为0并返回，1代表压缩，0代表不压缩
    public static int clearCompressedFlag(final int flag) {
        return flag & (~COMPRESSED_FLAG);
    }
}
