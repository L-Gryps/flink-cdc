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

package org.apache.flink.cdc.connectors.xugu;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/** Test stream. */
public class XuGuTestBase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);

        SourceFunction<String> build =
                XuGuSource.<String>builder()
                        .subscribeName("xugu001")
                        .hostname("10.28.25.158")
                        .port(5138)
                        .username("SYSDBA")
                        .password("SYSDBA")
                        .databaseName("CDC_TEST")
                        .tableList("SYSDBA.XUGU_ALL_TYPE2")
                        .startupOptions(StartupOptions.initial())
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .build();

        DataStreamSource<String> stringDataStreamSource =
                env.addSource(build, "xugu-cdc").setParallelism(1);

        stringDataStreamSource.print();
        env.execute();
    }
}
