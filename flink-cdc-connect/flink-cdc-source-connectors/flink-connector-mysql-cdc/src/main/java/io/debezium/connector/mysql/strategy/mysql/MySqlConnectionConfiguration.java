/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
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
            "${protocol}://${hostname}:${port}/?useInformationSchema=true&nullCatalogMeansCurrent=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&connectTimeout=${connectTimeout}";
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

        StringBuilder jdbcUrlStringBuilder = new StringBuilder(JDBC_URL_PATTERN);
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
