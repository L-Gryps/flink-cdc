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

package org.apache.flink.cdc.connectors.sqlserver;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceBuilder;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.Test;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Testing MySQL {@link SqlServerSourceBuilder} */
public class SqlserverSourceTest {
    @Test
    public void SqlServerEventAll() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(300000);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(
                        env, EnvironmentSettings.newInstance().inStreamingMode().build());

        Properties properties = new Properties();
        SqlServerSourceBuilder.SqlServerIncrementalSource<String> build =
                SqlServerSourceBuilder.SqlServerIncrementalSource.<String>builder()
                        .hostname("10.28.23.188")
                        .port(1433)
                        .username("u_migrate")
                        .password("123456")
                        .databaseList("d_migrate")
                        .tableList("dbo.products")
                        .serverTimeZone("Asia/Shanghai")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .chunkKeyColumn("id")
                        //                .includeSchemaChanges(true)
                        .startupOptions(StartupOptions.initial())
                        .skipSnapshotBackfill(true)
                        .build();
        env.fromSource(build, WatermarkStrategy.noWatermarks(), "mssqlcdc").print();
        env.execute();
    }

    @Test
    public void test1() {
        String mysqlUrl =
                "jdbc:mysql://localhost4:3306/old_database_name?useSSL=false&serverTimezone=UTC";
        String newDatabaseName = "new_database_name";

        String updatedUrl = replaceDatabaseName(mysqlUrl, newDatabaseName);
        System.out.println(updatedUrl);
    }

    public static String replaceDatabaseName(String url, String newDatabaseName) {
        // 正则表达式匹配MySQL URL中的数据库名称部分
        String patter = "(jdbc:+[a-zA-Z0-9-//.]+://[^/]+:\\d+/)([^?]+)";
        Pattern pattern = Pattern.compile(patter);
        Matcher matcher = pattern.matcher(url);
        String jdbcUrl =
                matcher.find()
                        ? matcher.group(1) + newDatabaseName + url.substring(matcher.end(2))
                        : url;
        // 替换数据库名称
        return jdbcUrl;
    }
}
