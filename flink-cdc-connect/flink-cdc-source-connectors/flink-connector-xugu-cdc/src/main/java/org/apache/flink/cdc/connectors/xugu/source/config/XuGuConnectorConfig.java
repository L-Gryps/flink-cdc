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

package org.apache.flink.cdc.connectors.xugu.source.config;

import org.apache.flink.cdc.connectors.xugu.source.offset.XuGuSourceInfoStructMaker;

import io.debezium.config.Configuration;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Tables;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static io.debezium.jdbc.JdbcConfiguration.DATABASE;

/** Debezium connector config. */
public class XuGuConnectorConfig extends RelationalDatabaseConnectorConfig {

    protected static final int DEFAULT_SNAPSHOT_FETCH_SIZE = Integer.MIN_VALUE;
    protected static final List<String> BUILT_IN_DB_NAMES =
            Collections.unmodifiableList(Arrays.asList("SYSSSO", "SYSAUDITOR", "GUEST"));

    private final String serverTimeZone;

    public XuGuConnectorConfig(String serverTimeZone, Properties properties) {
        super(
                Configuration.from(properties),
                Tables.TableFilter.fromPredicate(
                        tableId -> !BUILT_IN_DB_NAMES.contains(tableId.schema())),
                x -> x.schema() + "." + x.table(),
                DEFAULT_SNAPSHOT_FETCH_SIZE,
                ColumnFilterMode.SCHEMA,
                false);
        this.serverTimeZone = serverTimeZone;
    }

    public String getServerTimeZone() {
        return serverTimeZone;
    }

    public String databaseName() {
        return getJdbcConfig().getString(DATABASE);
    }

    @Override
    public String getConnectorName() {
        return "xugu";
    }

    @Override
    public String getContextName() {
        return "XuGu";
    }

    @Override
    protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
        return new XuGuSourceInfoStructMaker();
    }
}
