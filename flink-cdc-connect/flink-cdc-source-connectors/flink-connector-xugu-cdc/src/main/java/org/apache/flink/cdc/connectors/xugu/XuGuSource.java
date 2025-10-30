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

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.xugu.source.XuGuRichSourceFunction;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.xugudb.binlog.client.config.XGReadConnectionConfig;
import com.xugudb.binlog.client.enums.StartMode;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A builder to build a SourceFunction which can read snapshot and change events of OceanBase. */
@PublicEvolving
public class XuGuSource {

    protected static final String LOGICAL_NAME = "xugu_cdc_source";

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /** Builder class of {@link XuGuSource}. */
    public static class Builder<T> {

        // common config
        private StartupOptions startupOptions;
        private String username;
        private String password;
        private String databaseName;
        private String[] tableList;
        private String serverTimeZone;
        private Duration connectTimeout;

        // snapshot reading config
        private String hostname;
        private Integer port;
        private String jdbcDriver;
        private Properties jdbcProperties;

        // incremental reading config
        private String subscribeName;
        private Long startupTimestamp;
        private Properties xgcdcProperties;
        private Properties debeziumProperties;
        private String url;

        private DebeziumDeserializationSchema<T> deserializer;

        public Builder<T> startupOptions(StartupOptions startupOptions) {
            this.startupOptions = startupOptions;
            return this;
        }

        public Builder<T> url(String url) {
            this.url = url;
            return this;
        }

        public Builder<T> username(String username) {
            this.username = username;
            return this;
        }

        public Builder<T> password(String password) {
            this.password = password;
            return this;
        }

        public Builder<T> databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public Builder<T> tableList(String... tableList) {
            this.tableList = tableList;
            return this;
        }

        public Builder<T> serverTimeZone(String serverTimeZone) {
            this.serverTimeZone = serverTimeZone;
            return this;
        }

        public Builder<T> connectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder<T> hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public Builder<T> port(int port) {
            this.port = port;
            return this;
        }

        public Builder<T> jdbcDriver(String jdbcDriver) {
            this.jdbcDriver = jdbcDriver;
            return this;
        }

        public Builder<T> jdbcProperties(Properties jdbcProperties) {
            this.jdbcProperties = jdbcProperties;
            return this;
        }

        public Builder<T> subscribeName(String subscribeName) {
            this.subscribeName = subscribeName;
            return this;
        }

        public Builder<T> startupTimestamp(Long startupTimestamp) {
            this.startupTimestamp = startupTimestamp;
            return this;
        }

        public Builder<T> xgcdcProperties(Properties xgcdcProperties) {
            this.xgcdcProperties = xgcdcProperties;
            return this;
        }

        public Builder<T> debeziumProperties(Properties debeziumProperties) {
            this.debeziumProperties = debeziumProperties;
            return this;
        }

        public Builder<T> deserializer(DebeziumDeserializationSchema<T> deserializer) {
            this.deserializer = deserializer;
            return this;
        }

        public SourceFunction<T> build() {
            checkNotNull(username, "username shouldn't be null");
            checkNotNull(password, "password shouldn't be null");
            checkNotNull(hostname, "hostname shouldn't be null");
            checkNotNull(port, "port shouldn't be null");
            checkNotNull(databaseName, "database-name shouldn't be null");
            checkNotNull(tableList, "table-list shouldn't be null");
            checkNotNull(subscribeName, "subscribeName shouldn't be null");

            if (debeziumProperties == null || debeziumProperties.isEmpty()) {
                Properties properties = new Properties();
                properties.setProperty("topic.prefix", LOGICAL_NAME);
                properties.setProperty("decimal.handling.mode", "precise");
                debeziumProperties = properties;
            }

            if (startupOptions == null) {
                startupOptions = StartupOptions.initial();
            }

            if (jdbcDriver == null) {
                jdbcDriver = "com.xugu.cloudjdbc.Driver";
            }

            if (connectTimeout == null) {
                connectTimeout = Duration.ofSeconds(30);
            }

            if (serverTimeZone == null) {
                serverTimeZone = ZoneId.systemDefault().getId();
            }

            XGReadConnectionConfig xgReadConnectionConfig = null;

            if (!startupOptions.isSnapshotOnly()) {

                xgReadConnectionConfig = new XGReadConnectionConfig();

                xgReadConnectionConfig.setHost(hostname);
                xgReadConnectionConfig.setPort(port);
                xgReadConnectionConfig.setUser(username);
                xgReadConnectionConfig.setPassword(password);
                xgReadConnectionConfig.setSubscribeName(subscribeName);
                xgReadConnectionConfig.setDatabase(databaseName);
                xgReadConnectionConfig.setStartTimestamp(startupTimestamp);
                xgReadConnectionConfig.setIncludeTables(Arrays.asList(tableList));
                xgReadConnectionConfig.setTimeZone(serverTimeZone);

                switch (startupOptions.startupMode) {
                    case EARLIEST_OFFSET:
                        xgReadConnectionConfig.setStartMode(StartMode.EARLIEST_OFFSET);
                        break;
                    case LATEST_OFFSET:
                        xgReadConnectionConfig.setStartMode(StartMode.LATEST_OFFSET);
                        break;
                    case SNAPSHOT:
                        break;
                    case INITIAL:
                    case TIMESTAMP:
                        xgReadConnectionConfig.setStartMode(StartMode.TIMESTAMP);
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                startupOptions.startupMode + " mode is not supported.");
                }

                if (xgcdcProperties != null && !xgcdcProperties.isEmpty()) {
                    Map<String, String> extraConfigs = new HashMap<>();
                    xgcdcProperties.forEach((k, v) -> extraConfigs.put(k.toString(), v.toString()));
                    xgReadConnectionConfig.setExtraConfigs(extraConfigs);
                }

                if (!url.isEmpty()) {
                    xgReadConnectionConfig.setUrl(url);
                }
            }

            return new XuGuRichSourceFunction<>(
                    startupOptions,
                    username,
                    password,
                    databaseName,
                    tableList,
                    serverTimeZone,
                    connectTimeout,
                    hostname,
                    port,
                    jdbcDriver,
                    jdbcProperties,
                    xgReadConnectionConfig,
                    debeziumProperties,
                    deserializer);
        }
    }
}
