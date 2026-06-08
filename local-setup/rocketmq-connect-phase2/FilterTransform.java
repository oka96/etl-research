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

package org.apache.rocketmq.connect.file;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.Transform;
import io.openmessaging.connector.api.data.ConnectRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterTransform implements Transform<ConnectRecord> {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.FILE_CONNECTOR);
    private static final String DEFAULT_PIPELINE_NAME = "rocketmq-connect-k8s-phase2";

    private KeyValue keyValue;
    private String pipelineName = DEFAULT_PIPELINE_NAME;

    @Override public ConnectRecord doTransform(ConnectRecord record) {
        Object data = record.getData();
        if (data == null) {
            return null;
        }
        try {
            JSONObject event = JSON.parseObject(String.valueOf(data));
            double amount = event.getDoubleValue("amount");
            boolean accepted = "wallet.payment.authorized".equals(event.getString("event_type")) && amount >= 100.0;
            if (!accepted) {
                return null;
            }
            event.put("amount", amount);
            event.put("risk_tier", amount >= 1000.0 ? "HIGH" : "STANDARD");
            event.put("pipeline", pipelineName);
            record.setData(event.toJSONString());
            return record;
        } catch (Exception e) {
            log.warn("Skipping invalid wallet JSON record {}", data, e);
            return null;
        }
    }

    @Override
    public void start(KeyValue config) {
        this.keyValue = config;
        String configuredPipeline = config == null ? null : config.getString("pipeline");
        if (configuredPipeline != null && !configuredPipeline.trim().isEmpty()) {
            this.pipelineName = configuredPipeline;
        }
        log.info("transform config {}", this.keyValue);
    }

    @Override
    public void stop() {
    }
}
