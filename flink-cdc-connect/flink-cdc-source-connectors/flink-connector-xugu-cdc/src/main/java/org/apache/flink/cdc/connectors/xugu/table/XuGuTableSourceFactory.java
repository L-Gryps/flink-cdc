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

package org.apache.flink.cdc.connectors.xugu.table;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.xugu.utils.XuGuUtils;
import org.apache.flink.cdc.connectors.xugu.utils.OptionUtils;
import org.apache.flink.cdc.debezium.table.DebeziumOptions;
import org.apache.flink.cdc.debezium.utils.JdbcUrlUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_STARTUP_MODE;

/** Factory for creating configured instance of {@link XuGuTableSource}. */
public class XuGuTableSourceFactory implements DynamicTableSourceFactory {

    private static final String IDENTIFIER = "xugu-cdc";

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Username to be used when connecting to OceanBase.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password to be used when connecting to OceanBase.");

    public static final ConfigOption<String> SUBSCRIBE_NAME =
            ConfigOptions.key("subscribe-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("subscribe name of xugu to binlog.");

    public static final ConfigOption<String> DATABASE_NAME =
            ConfigOptions.key("database-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Database name of xugu to monitor, should be regular expression. Only can be used with 'initial' mode.");
    public static final ConfigOption<String> SCHEMA_NAME =
            ConfigOptions.key("schema-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "schema name of xugu to monitor, should be regular expression. Only can be used with 'initial' mode.");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "List of  names of tables, separated by commas, e.g. \"table1\".");

    public static final ConfigOption<String> SERVER_TIME_ZONE =
            ConfigOptions.key("server-time-zone")
                    .stringType()
                    .defaultValue("+00:00")
                    .withDescription("The session time zone in database server.");

    public static final ConfigOption<Duration> CONNECT_TIMEOUT =
            ConfigOptions.key("connect.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "The maximum time that the connector should wait after trying to connect to the database server or log proxy server before timing out.");

    public static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "IP address or hostname of the xugu database server or xugu proxy server.");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Integer port number of xugu database server or xugu proxy server.");

    public static final ConfigOption<String> JDBC_DRIVER =
            ConfigOptions.key("jdbc.driver")
                    .stringType()
                    .defaultValue("com.xugu.cloudjdbc.Driver")
                    .withDescription(
                            "JDBC driver class name, use 'com.xugu.cloudjdbc.Driver' by default.");


    public static final ConfigOption<Long> SCAN_STARTUP_TIMESTAMP =
            ConfigOptions.key("scan.startup.timestamp")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp in seconds used in case of \"timestamp\" startup mode.");


    public static final String XGCDC_PROPERTIES = "xgcdc.properties.";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validateExcept(
                JdbcUrlUtils.PROPERTIES_PREFIX,
                XGCDC_PROPERTIES,
                DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX);

        ResolvedSchema physicalSchema = context.getCatalogTable().getResolvedSchema();

        ReadableConfig config = helper.getOptions();

        StartupOptions startupOptions = getStartupOptions(config);

        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);
        String subscribeName = config.get(SUBSCRIBE_NAME);
        String databaseName = config.get(DATABASE_NAME);
        String schemaName = config.get(SCHEMA_NAME);
        String tableName = config.get(TABLE_NAME);

        String serverTimeZone = config.get(SERVER_TIME_ZONE);
        Duration connectTimeout = config.get(CONNECT_TIMEOUT);

        String hostname = config.get(HOSTNAME);
        Integer port = config.get(PORT);
        String jdbcDriver = config.get(JDBC_DRIVER);
        validateJdbcDriver(jdbcDriver);
        Long startupTimestamp = config.get(SCAN_STARTUP_TIMESTAMP);

        OptionUtils.printOptions(IDENTIFIER, ((Configuration) config).toMap());

        return new XuGuTableSource(
                physicalSchema,
                startupOptions,
                username,
                password,
                subscribeName,
                databaseName,
                schemaName,
                tableName,
                serverTimeZone,
                connectTimeout,
                hostname,
                port,
                jdbcDriver,
                JdbcUrlUtils.getJdbcProperties(context.getCatalogTable().getOptions()),
                startupTimestamp,
                getProperties(context.getCatalogTable().getOptions(), XGCDC_PROPERTIES),
                DebeziumOptions.getDebeziumProperties(context.getCatalogTable().getOptions()));
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(HOSTNAME);
        options.add(PORT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_STARTUP_TIMESTAMP);
        options.add(DATABASE_NAME);
        options.add(SUBSCRIBE_NAME);
        options.add(TABLE_NAME);
        options.add(SCHEMA_NAME);
        options.add(JDBC_DRIVER);
        options.add(CONNECT_TIMEOUT);
        options.add(SERVER_TIME_ZONE);
        return options;
    }

    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
    private static final String SCAN_STARTUP_MODE_VALUE_SNAPSHOT = "snapshot";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_TIMESTAMP = "timestamp";

    private static StartupOptions getStartupOptions(ReadableConfig config) {
        String modeString = config.get(SCAN_STARTUP_MODE);

        switch (modeString.toLowerCase()) {
            case SCAN_STARTUP_MODE_VALUE_INITIAL:
                return StartupOptions.initial();
            case SCAN_STARTUP_MODE_VALUE_SNAPSHOT:
                return StartupOptions.snapshot();
            case SCAN_STARTUP_MODE_VALUE_LATEST:
                return StartupOptions.latest();
            case SCAN_STARTUP_MODE_VALUE_TIMESTAMP:
                if (config.get(SCAN_STARTUP_TIMESTAMP) != null) {
                    return StartupOptions.timestamp(config.get(SCAN_STARTUP_TIMESTAMP) * 1000);
                }
                throw new ValidationException(
                        String.format(
                                "Option '%s' should not be empty", SCAN_STARTUP_TIMESTAMP.key()));

            default:
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s, %s, %s], but was: %s",
                                SCAN_STARTUP_MODE.key(),
                                SCAN_STARTUP_MODE_VALUE_INITIAL,
                                SCAN_STARTUP_MODE_VALUE_SNAPSHOT,
                                SCAN_STARTUP_MODE_VALUE_LATEST,
                                SCAN_STARTUP_MODE_VALUE_TIMESTAMP,
                                modeString));
        }
    }

    private void validateJdbcDriver(String jdbcDriver) {
        Objects.requireNonNull(jdbcDriver, "'jdbc.driver' is required.");
        try {
            Class.forName(jdbcDriver);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Jdbc driver class not found", e);
        }
    }

    private Properties getProperties(Map<String, String> tableOptions, String prefix) {
        Properties properties = new Properties();
        tableOptions.keySet().stream()
                .filter(key -> key.startsWith(prefix))
                .forEach(
                        key -> {
                            final String value = tableOptions.get(key);
                            final String subKey = key.substring(prefix.length());
                            properties.put(subKey, value);
                        });
        return properties;
    }
}
