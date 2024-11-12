/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.postgres.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.Test;

/** Testing MySQL {@link PostgresSourceBuilder} */
public class PostgresSourecTest {
    @Test
    public void PostgresEventAll() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(300000);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        env, EnvironmentSettings.newInstance().inStreamingMode().build());

        PostgresSourceBuilder.PostgresIncrementalSource<String> build =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .hostname("192.168.2.223")
                        .port(5432)
                        .username("postgres")
                        .password("postgres")
                        .database("postgres")
                        .schemaList("inventory")
                        .tableList("inventory.full_types")
                        //                .chunkKeyColumn()
                        .deserializer(new StringDebeziumDeserializationSchema())
                        .splitSize(2)
                        .includeSchemaChanges(true)
                        .slotName("flink3")
                        .decodingPluginName("pgoutput")
                        .startupOptions(StartupOptions.initial())
                        .build();
        env.fromSource(build, WatermarkStrategy.noWatermarks(), "pgcdc").print();
        env.execute();
    }
}
