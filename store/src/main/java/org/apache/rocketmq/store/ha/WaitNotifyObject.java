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
package org.apache.rocketmq.store.ha;

import org.apache.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

//套路都一样
public class WaitNotifyObject {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    protected final HashMap<Long/* thread id */, Boolean/* notified */> waitingThreadTable = new HashMap<Long, Boolean>(16);

    protected volatile boolean hasNotified = false;                 //是否已经通知过

    public void wakeup() {
        synchronized (this) {
            if (!this.hasNotified) {
                this.hasNotified = true;
                this.notify();                                       //唤醒一个等待此锁的线程
            }
        }
    }

    //将hasNotified置为false
    //调用onWaitEnd()   是否wait
    protected void waitForRunning(long interval) {
        synchronized (this) {                             //如果被通知过了，将hasNotified置为false，直接调用onWaitEnd，
            if (this.hasNotified) {
                this.hasNotified = false;
                this.onWaitEnd();
                return;
            }

            try {                                          //如果没有被通知过，则wait一段时间，然后将hasNotified置为false，调用onWaitEnd
                this.wait(interval);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            } finally {
                this.hasNotified = false;
                this.onWaitEnd();
            }
        }
    }

    protected void onWaitEnd() {
    }

    //将thread值为false时，唤醒所有在this上等待的线程
    public void wakeupAll() {
        synchronized (this) {
            boolean needNotify = false;

            for (Boolean value : this.waitingThreadTable.values()) {
                needNotify = needNotify || !value;
                value = true;
            }

            if (needNotify) {
                this.notifyAll();
            }
        }
    }

    public void allWaitForRunning(long interval) {
        long currentThreadId = Thread.currentThread().getId();
        synchronized (this) {
            Boolean notified = this.waitingThreadTable.get(currentThreadId);
            //如果当前线程 值为true ，置为false，调用onWaitEnd
            if (notified != null && notified) {
                this.waitingThreadTable.put(currentThreadId, false);
                this.onWaitEnd();
                return;
            }

            //否则，沉睡一段时间
            try {
                this.wait(interval);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            } finally {
                this.waitingThreadTable.put(currentThreadId, false);
                this.onWaitEnd();
            }
        }
    }
}
