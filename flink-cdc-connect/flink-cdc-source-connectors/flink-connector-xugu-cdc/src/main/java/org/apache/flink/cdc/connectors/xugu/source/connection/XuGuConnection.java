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

package org.apache.flink.cdc.connectors.xugu.source.connection;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Attribute;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** {@link JdbcConnection} extension to be used with OceanBase server. */
public class XuGuConnection extends JdbcConnection {

    private static final Logger LOG = LoggerFactory.getLogger(XuGuConnection.class);

    private static final Properties DEFAULT_JDBC_PROPERTIES = initializeDefaultJdbcProperties();
    private static final String URL_PATTERN =
            "jdbc:xugu://${hostname}:${port}/${dbname}?";

    private static final int TYPE_BINARY_FLOAT = 100;
    private static final int TYPE_BINARY_DOUBLE = 101;
    private static final int TYPE_TIMESTAMP_WITH_TIME_ZONE = -101;
    private static final int TYPE_TIMESTAMP_WITH_LOCAL_TIME_ZONE = -102;
    private static final int TYPE_INTERVAL_YEAR_TO_MONTH = -103;
    private static final int TYPE_INTERVAL_DAY_TO_SECOND = -104;

    public XuGuConnection(
            String hostname,
            Integer port,
            String user,
            String password,
            String dbName,
            Duration timeout,
            String jdbcDriver,
            Properties jdbcProperties,
            ClassLoader classLoader) {
        super(
                config(hostname, port, user, password, dbName,timeout),
                JdbcConnection.patternBasedFactory(
                        formatJdbcUrl(jdbcDriver, jdbcProperties), jdbcDriver, classLoader),
                getQuote() + "",
                getQuote() + "");
    }

    private static JdbcConfiguration config(
            String hostname, Integer port, String user, String password,String dbName, Duration timeout) {
        return JdbcConfiguration.create()
                .with("hostname", hostname)
                .with("port", port)
                .with("user", user)
                .with("password", password)
                .with("dbname", dbName)
                .with("connect_timeout", timeout == null ? 3600000 : timeout.toMillis())
                .build();
    }

    private static String formatJdbcUrl(String jdbcDriver, Properties jdbcProperties) {
        Properties combinedProperties = new Properties();
        combinedProperties.putAll(DEFAULT_JDBC_PROPERTIES);
        if (jdbcProperties != null) {
            combinedProperties.putAll(jdbcProperties);
        }
        String urlPattern = URL_PATTERN;
        StringBuilder jdbcUrlStringBuilder = new StringBuilder(urlPattern);
        combinedProperties.forEach(
                (key, value) -> {
                    jdbcUrlStringBuilder.append("&").append(key).append("=").append(value);
                });
        return jdbcUrlStringBuilder.toString();
    }

    private static Properties initializeDefaultJdbcProperties() {
        Properties defaultJdbcProperties = new Properties();
        defaultJdbcProperties.setProperty("time_zone", "UTC+8");
        defaultJdbcProperties.setProperty("char_set", "UTF8");
        return defaultJdbcProperties;
    }

    private static char getQuote() {
        return  '"';
    }

    /**
     * Get current timestamp number in seconds.
     *
     * @return current timestamp number.
     * @throws SQLException If a database access error occurs.
     */
    public long getCurrentTimestampSss() throws SQLException {
        return getCurrentTimestamp().orElseThrow(IllegalStateException::new).toEpochMilli();
    }

    /**
     * Get table list by database name pattern and table name pattern.
     *
     * @param dbPattern Database name pattern.
     * @param tbPattern Table name pattern.
     * @return TableId list.
     * @throws SQLException If a database access error occurs.
     */
    public List<TableId> getTables(String dbPattern, String tbPattern) throws SQLException {
        List<TableId> result = new ArrayList<>();
        DatabaseMetaData metaData = connection().getMetaData();

        List<String> schemaNames = getResultList(metaData.getSchemas(), "TABLE_SCHEM");
        schemaNames =
                schemaNames.stream()
                        .filter(schemaName -> Pattern.matches(dbPattern, schemaName))
                        .collect(Collectors.toList());
        for (String schemaName : schemaNames) {
            List<String> tableNames =
                    getResultList(
                            metaData.getTables(
                                    null, schemaName, null, supportedTableTypes()),
                            "TABLE_NAME");
            tableNames.stream()
                    .filter(tbName -> Pattern.matches(tbPattern, tbName))
                    .forEach(tbName -> result.add(new TableId(null, schemaName, tbName)));
        }
        return result;
    }

    private List<String> getResultList(ResultSet resultSet, String columnName) throws SQLException {
        List<String> result = new ArrayList<>();
        while (resultSet.next()) {
            result.add(resultSet.getString(columnName));
        }
        return result;
    }

    @Override
    protected String[] supportedTableTypes() {
        return new String[] {"TABLE"};
    }

    @Override
    public String quotedTableIdString(TableId tableId) {
        return tableId.toQuotedString(getQuote());
    }

    public void readSchemaForCapturedTables(
            Tables tables,
            String databaseCatalog,
            String schemaNamePattern,
            Tables.ColumnNameFilter columnFilter,
            boolean removeTablesNotFoundInJdbc,
            Set<TableId> capturedTables)
            throws SQLException {

        Set<TableId> tableIdsBefore = new HashSet<>(tables.tableIds());

        DatabaseMetaData metadata = connection().getMetaData();
        Map<TableId, List<Column>> columnsByTable = new HashMap<>();
        Map<TableId, List<Attribute>> attributesByTable = new HashMap<>();

        for (TableId tableId : capturedTables) {
            try (ResultSet columnMetadata =
                    metadata.getColumns(
                            databaseCatalog, schemaNamePattern, tableId.table(), null)) {
                while (columnMetadata.next()) {
                    // add all whitelisted columns
                    readTableColumn(columnMetadata, tableId, columnFilter)
                            .ifPresent(
                                    column -> {
                                        columnsByTable
                                                .computeIfAbsent(tableId, t -> new ArrayList<>())
                                                .add(column.create());
                                    });
                }
                attributesByTable.putAll(getAttributeDetails(tableId));
            }
        }

        // Read the metadata for the primary keys ...
        for (Map.Entry<TableId, List<Column>> tableEntry : columnsByTable.entrySet()) {
            // First get the primary key information, which must be done for *each* table ...
            List<String> pkColumnNames = readPrimaryKeyNames(metadata, tableEntry.getKey());

            // Then define the table ...
            List<Column> columns = tableEntry.getValue();
            Collections.sort(columns);
            List<Attribute> attributes =
                    attributesByTable.getOrDefault(tableEntry.getKey(), Collections.emptyList());
            tables.overwriteTable(tableEntry.getKey(), columns, pkColumnNames, null, attributes);
        }

        if (removeTablesNotFoundInJdbc) {
            // Remove any definitions for tables that were not found in the database metadata ...
            tableIdsBefore.removeAll(columnsByTable.keySet());
            tableIdsBefore.forEach(tables::removeTable);
        }
    }

    @Override
    protected int resolveNativeType(String typeName) {
        String upperCaseTypeName = typeName.toUpperCase();
        if (upperCaseTypeName.startsWith("JSON")) {
            return Types.VARCHAR;
        }
        if (upperCaseTypeName.startsWith("TIMESTAMP")) {
            if (upperCaseTypeName.contains("WITH TIME ZONE")) {
                return TYPE_TIMESTAMP_WITH_TIME_ZONE;
            }
            if (upperCaseTypeName.contains("WITH LOCAL TIME ZONE")) {
                return TYPE_TIMESTAMP_WITH_LOCAL_TIME_ZONE;
            }
            return Types.TIMESTAMP;
        }
        if (upperCaseTypeName.startsWith("INTERVAL")) {
            if (upperCaseTypeName.contains("TO MONTH")) {
                return TYPE_INTERVAL_YEAR_TO_MONTH;
            }
            if (upperCaseTypeName.contains("TO SECOND")) {
                return TYPE_INTERVAL_DAY_TO_SECOND;
            }
        }
        return Column.UNSET_INT_VALUE;
    }

    @Override
    protected int resolveJdbcType(int metadataJdbcType, int nativeType) {
        switch (metadataJdbcType) {
            case TYPE_BINARY_FLOAT:
                return Types.REAL;
            case TYPE_BINARY_DOUBLE:
                return Types.DOUBLE;
            case TYPE_TIMESTAMP_WITH_TIME_ZONE:
            case TYPE_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TYPE_INTERVAL_YEAR_TO_MONTH:
            case TYPE_INTERVAL_DAY_TO_SECOND:
                return Types.OTHER;
            default:
                return nativeType == Column.UNSET_INT_VALUE ? metadataJdbcType : nativeType;
        }
    }

//    @Override
//    protected Optional<ColumnEditor> readTableColumn(ResultSet columnMetadata, TableId tableId, Tables.ColumnNameFilter columnFilter) throws SQLException {
//        return doReadTableColumn(columnMetadata, tableId, columnFilter);
//    }
//
//    private Optional<ColumnEditor> doReadTableColumn(ResultSet columnMetadata, TableId tableId, Tables.ColumnNameFilter columnFilter)
//            throws SQLException {
//        final String defaultValue = columnMetadata.getString(13);
//
//        final String columnName = columnMetadata.getString(4);
//        if (columnFilter == null || columnFilter.matches(tableId.catalog(), tableId.schema(), tableId.table(), columnName)) {
//            ColumnEditor column = Column.editor().name(columnName);
//            column.type(columnMetadata.getString(6));
//            column.length(columnMetadata.getInt(7));
//            if (columnMetadata.getObject(9) != null) {
//                column.scale(columnMetadata.getInt(9));
//            }
//            column.optional(isNullable(columnMetadata.getInt(11)));
//            column.position(columnMetadata.getInt(17));
//            column.autoIncremented("YES".equalsIgnoreCase(columnMetadata.getString(23)));
//            String autogenerated = null;
//            try {
//                autogenerated = columnMetadata.getString(24);
//            }
//            catch (SQLException e) {
//                // ignore, some drivers don't have this index - e.g. Postgres
//            }
//            column.generated("YES".equalsIgnoreCase(autogenerated));
//
//            column.nativeType(resolveNativeType(column.typeName()));
//            column.jdbcType(resolveJdbcType(columnMetadata.getInt(5), column.nativeType()));
//
//            // Allow implementation to make column changes if required before being added to table
//            column = overrideColumn(column);
//
//            final String defaultValueExpression = columnMetadata.getString(13);
//            if (defaultValueExpression != null && getDefaultValueConverter().supportConversion(column.typeName())) {
//                column.defaultValueExpression(defaultValueExpression);
//            }
//
//            return Optional.of(column);
//        }
//
//        return Optional.empty();
//    }
}
