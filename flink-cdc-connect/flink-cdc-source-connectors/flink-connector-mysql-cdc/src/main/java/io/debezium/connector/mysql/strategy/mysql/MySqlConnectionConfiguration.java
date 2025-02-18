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

package io.debezium.connector.mysql.strategy.mysql;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.strategy.AbstractConnectionConfiguration;
import io.debezium.jdbc.JdbcConnection;

import java.util.Properties;

/**
 * Copied from Debezium project(2.5.4.final) to add custom jdbc properties in the jdbc url. The new
 * parameter {@code jdbcProperties} in the constructor of {@link MySqlConnectionConfiguration} will
 * be used to generate the jdbc url pattern, and may overwrite the default value.
 *
 * <p>*
 *
 * <p>Line 31 ~ 37: Add new constant and field {@code urlPattern} to {@link *
 * MySqlConnectionConfiguration}. * *
 *
 * <p>Line 43 ~ 48: Init new field {@code urlPattern} in {@link MySqlConnectionConfiguration}. * *
 *
 * <p>Line 51 ~ 69: Add some methods helping to generate the url pattern and add default values.
 */
public class MySqlConnectionConfiguration extends AbstractConnectionConfiguration {

    private static final String JDBC_PROPERTY_CONNECTION_TIME_ZONE = "connectionTimeZone";
    public static final String JDBC_URL_PATTERN =
            "${protocol}://${hostname}:${port}/?useSSL=false&useInformationSchema=true&nullCatalogMeansCurrent=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&connectTimeout=${connectTimeout}";
    private final JdbcConnection.ConnectionFactory factory;
    private final String urlPattern;

    public MySqlConnectionConfiguration(Configuration config) {
        this(config, new Properties());
    }

    public MySqlConnectionConfiguration(Configuration config, Properties jdbcProperties) {
        super(config);
        this.urlPattern = formatJdbcUrl(jdbcProperties);
        String driverClassName = super.config().getString(MySqlConnectorConfig.JDBC_DRIVER);
        Field protocol = MySqlConnectorConfig.JDBC_PROTOCOL;
        factory =
                JdbcConnection.patternBasedFactory(
                        urlPattern, driverClassName, getClass().getClassLoader(), protocol);
    }

    private String formatJdbcUrl(Properties jdbcProperties) {
        Properties combinedProperties = new Properties();
        combinedProperties.putAll(jdbcProperties);

        StringBuilder jdbcUrlStringBuilder =
                jdbcProperties.getProperty("useSSL") == null
                        ? new StringBuilder(JDBC_URL_PATTERN)
                        : new StringBuilder(URL_PATTERN);
        combinedProperties.forEach(
                (key, value) -> {
                    jdbcUrlStringBuilder.append("&").append(key).append("=").append(value);
                });

        return jdbcUrlStringBuilder.toString();
    }

    public String getUrlPattern() {
        return urlPattern;
    }

    @Override
    public JdbcConnection.ConnectionFactory factory() {
        return factory;
    }

    @Override
    protected String getConnectionTimeZonePropertyName() {
        return JDBC_PROPERTY_CONNECTION_TIME_ZONE;
    }

    @Override
    protected String resolveConnectionTimeZone(Configuration dbConfig) {
        // Debezium by default expects time zoned data delivered in server timezone
        String connectionTimeZone = dbConfig.getString(JDBC_PROPERTY_CONNECTION_TIME_ZONE);
        return connectionTimeZone != null ? connectionTimeZone : "SERVER";
        // return !Strings.isNullOrBlank(connectionTimeZone) ? connectionTimeZone : "SERVER";
    }
}
