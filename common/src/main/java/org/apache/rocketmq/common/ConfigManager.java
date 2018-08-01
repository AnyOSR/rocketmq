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
package org.apache.rocketmq.common;

import java.io.IOException;
import org.apache.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//配置管理基类，主要是加载配置文件读数据，或者持久化到文件
//模板设计模式
//基类抽取共同行为，子类实现具体的业务逻辑(在这儿就是获取配置文件的fileName)
public abstract class ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    //
    public abstract String encode();

    //返回配置文件文件名
    public abstract String configFilePath();

    //
    public abstract String encode(final boolean prettyFormat);

    //
    public abstract void decode(final String jsonString);

    //首先从配置的文件读取数据
    //如果读取不到或者异常，则从备份文件读取数据
    //如果读取到数据，则对数据进行decode()操作
    public boolean load() {
        String fileName = null;
        try {
            fileName = this.configFilePath();
            String jsonString = MixAll.file2String(fileName);

            if (null == jsonString || jsonString.length() == 0) {
                return this.loadBak();
            } else {
                this.decode(jsonString);
                log.info("load {} OK", fileName);
                return true;
            }
        } catch (Exception e) {
            log.error("load [{}] failed, and try to load backup file", fileName, e);
            return this.loadBak();
        }
    }

    private boolean loadBak() {
        String fileName = null;
        try {
            fileName = this.configFilePath();
            String jsonString = MixAll.file2String(fileName + ".bak");
            if (jsonString != null && jsonString.length() > 0) {
                this.decode(jsonString);
                log.info("load [{}] OK", fileName);
                return true;
            }
        } catch (Exception e) {
            log.error("load [{}] Failed", fileName, e);
            return false;
        }

        return true;
    }

    //首先encode
    //然后将数据持久化到文件
    public synchronized void persist() {
        String jsonString = this.encode(true);
        if (jsonString != null) {
            String fileName = this.configFilePath();
            try {
                MixAll.string2File(jsonString, fileName);
            } catch (IOException e) {
                log.error("persist file [{}] exception", fileName, e);
            }
        }
    }


}
