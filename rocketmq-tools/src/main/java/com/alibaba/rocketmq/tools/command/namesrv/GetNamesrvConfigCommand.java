/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.rocketmq.tools.command.namesrv;

import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author xigu.lx
 */
public class GetNamesrvConfigCommand implements SubCommand {

    @Override
    public String commandName() {
        return "getNamesrvConfig";
    }

    @Override
    public String commandDesc() {
        return "Get configs of name server.";
    }

    @Override
    public Options buildCommandlineOptions(final Options options) {
        return options;
    }

    @Override
    public void execute(final CommandLine commandLine, final Options options, final RPCHook rpcHook) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            // servers
            String servers = commandLine.getOptionValue('n');
            List<String> serverList = null;
            if (servers != null && servers.length() > 0) {
                String[] serverArray = servers.trim().split(";");

                if (serverArray != null && serverArray.length > 0) {
                    serverList = Arrays.asList(serverArray);
                }
            }

            defaultMQAdminExt.start();

            Map<String, Properties> nameServerConfigs = defaultMQAdminExt.getNameServerConfig(serverList);

            for (String server : nameServerConfigs.keySet()) {
                System.out.printf("============%s============\n",
                        server);
                for (Object key : nameServerConfigs.get(server).keySet()) {
                    System.out.printf("%-50s=  %s\n", key, nameServerConfigs.get(server).get(key));
                }
            }
            return;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}