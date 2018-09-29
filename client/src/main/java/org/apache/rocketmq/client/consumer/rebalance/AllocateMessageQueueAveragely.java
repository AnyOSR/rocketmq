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
package org.apache.rocketmq.client.consumer.rebalance;

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Average Hashing queue algorithm
 */
public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {
    private final Logger log = ClientLogger.getLog();

    public static void main(String[] args) {
        List<String> cidAll = new ArrayList<String>();
        List<MessageQueue> messageQueues = new LinkedList<MessageQueue>();
        int clientTotal = 16;
        int mqSize = 40;
        String currentCID = "client_9";
        for (int i = 0; i < clientTotal; i++) {
            cidAll.add("client_" + i);
        }
        for(int j=0;j<mqSize;j++){
            MessageQueue temp = new MessageQueue();
            temp.setQueueId(j);
            messageQueues.add(temp);
        }

        new AllocateMessageQueueAveragely().allocate("a",currentCID,messageQueues,cidAll);

    }

    //consumerGroup currentClientId 所有的messageQueue 所有的clientId
    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll, List<String> cidAll) {
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}", consumerGroup, currentCID, cidAll);
            return result;
        }

        int index = cidAll.indexOf(currentCID);    //当前clientID在cidAll里面的下标
        int mod = mqAll.size() % cidAll.size();    //假如将messageQueue从第一个开始给所有的clientID平分，分完之后还剩下的没分的messageQueue个数

        // 0000 0000 0000 0000       当前行，0的个数是cidAll.size()  所有行，0的总个数为mqAll.size()
        // 0000 0000 0000 0000       mod的值为 最后一行0的个数   index的值为currentId在cidAll中的下标，可能出现在任意一个位置
        // 0000 0000
        //如果mqAll.size() <= cidAll.size() 肯定为1
        //只有在mod != 0且index+1 <= mod的时候，即 mod >0 && index < mod的时候，才能+1 ，否则都是mqAll.size() / cidAll.size())
        //计算的是当前clientId分配的messageQueue个数
        int averageSize = mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size() + 1 : mqAll.size() / cidAll.size());

        //然后计算当前clientId被分配的messageQueue下标范围
        //行变列 列变行
        //0  3  6  9   12  15  18  21  24  26  28
        //1  4  7  10  13  16  19  22  25  27  29
        //2  5  8  11  14  17  20  23
        //行变列 列变行之后，第一行的索引值
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;

        //？？
        // startIndex >= index = cidAll.indexOf(currentCID) 有可能 >= mqAll.size()
        // 有可能，当client的个数比 messageQueues的总数多的时候，range <= 0
        //这时候竟然某个client取不到messageQueue，科学？
        //还是上层有硬性校验？
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        //取range个元素，从startIndex开始取
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }
}
