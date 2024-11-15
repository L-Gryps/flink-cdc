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

import com.mysql.cj.CharsetMapping;
import io.debezium.DebeziumException;
import io.debezium.connector.mysql.GtidSet;
import io.debezium.connector.mysql.GtidUtils;
import io.debezium.connector.mysql.MySqlFieldReader;
import io.debezium.connector.mysql.MySqlTextProtocolFieldReader;
import io.debezium.connector.mysql.strategy.AbstractConnectorConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.function.Predicate;

/**
 * Copied from Debezium project(2.5.4.final) to add custom jdbc properties in the jdbc url. The new
 * parameter {@code jdbcProperties} in the constructor of {@link MySqlConnectionConfiguration} will
 * be used to generate the jdbc url pattern, and may overwrite the default value.
 *
 * <p>Line 46: Add field {@code urlPattern} in {@link MySqlConnection} and remove old pattern.
 *
 * <p>Line 52: Init {@code urlPattern} using the url pattern from {@link
 * MySqlConnectionConfiguration}.
 *
 * <p>Line 192: Generate the connection string by the new field {@code urlPattern}.
 */
public class MySqlConnection extends AbstractConnectorConnection {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(
                    io.debezium.connector.mysql.strategy.mysql.MySqlConnection.class);

    private final String urlPattern;

    public MySqlConnection(
            MySqlConnectionConfiguration connectionConfig, MySqlFieldReader fieldReader) {
        super(connectionConfig, fieldReader);
        this.urlPattern = connectionConfig.getUrlPattern();
    }

    /**
     * Creates a new connection using the supplied configuration.
     *
     * @param connectionConfig {@link MySqlConnectionConfiguration} instance, may not be null.
     */
    public MySqlConnection(MySqlConnectionConfiguration connectionConfig) {
        this(connectionConfig, new MySqlTextProtocolFieldReader(null));
    }

    @Override
    public boolean isGtidModeEnabled() {
        try {
            return queryAndMap(
                    "SHOW GLOBAL VARIABLES LIKE 'GTID_MODE'",
                    rs -> {
                        if (rs.next()) {
                            return "ON".equalsIgnoreCase(rs.getString(2));
                        }
                        return false;
                    });
        } catch (SQLException e) {
            throw new DebeziumException("Unexpected error while looking at GTID mode: ", e);
        }
    }

    @Override
    public GtidSet knownGtidSet() {
        try {
            return queryAndMap(
                    "SHOW MASTER STATUS",
                    rs -> {
                        if (rs.next() && rs.getMetaData().getColumnCount() > 4) {
                            return new MySqlGtidSet(
                                    rs.getString(
                                            5)); // GTID set, may be null, blank, or contain a GTID
                            // set
                        }
                        return new MySqlGtidSet("");
                    });
        } catch (SQLException e) {
            throw new DebeziumException("Unexpected error while looking at GTID mode: ", e);
        }
    }

    @Override
    public GtidSet subtractGtidSet(GtidSet set1, GtidSet set2) {
        try {
            return prepareQueryAndMap(
                    "SELECT GTID_SUBTRACT(?, ?)",
                    ps -> {
                        ps.setString(1, set1.toString());
                        ps.setString(2, set2.toString());
                    },
                    rs -> {
                        if (rs.next()) {
                            return new MySqlGtidSet(rs.getString(1));
                        }
                        return new MySqlGtidSet("");
                    });
        } catch (SQLException e) {
            throw new DebeziumException("Unexpected error while executing GTID_SUBTRACT: ", e);
        }
    }

    @Override
    public GtidSet purgedGtidSet() {
        try {
            return queryAndMap(
                    "SELECT @@global.gtid_purged",
                    rs -> {
                        if (rs.next() && rs.getMetaData().getColumnCount() > 0) {
                            return new MySqlGtidSet(
                                    rs.getString(
                                            1)); // GTID set, may be null, blank, or contain a GTID
                            // set
                        }
                        return new MySqlGtidSet("");
                    });
        } catch (SQLException e) {
            throw new DebeziumException(
                    "Unexpected error while looking at gtid_purged variable: ", e);
        }
    }

    @Override
    public GtidSet filterGtidSet(
            Predicate<String> gtidSourceFilter,
            String offsetGtids,
            GtidSet availableServerGtidSet,
            GtidSet purgedServerGtidSet) {
        String gtidStr = offsetGtids;
        if (gtidStr == null) {
            return null;
        }
        LOGGER.info("Attempting to generate a filtered GTID set");
        LOGGER.info("GTID set from previous recorded offset: {}", gtidStr);
        GtidSet filteredGtidSet = new MySqlGtidSet(gtidStr);
        if (gtidSourceFilter != null) {
            filteredGtidSet = filteredGtidSet.retainAll(gtidSourceFilter);
            LOGGER.info(
                    "GTID set after applying GTID source includes/excludes to previous recorded offset: {}",
                    filteredGtidSet);
        }
        LOGGER.info("GTID set available on server: {}", availableServerGtidSet);

        final GtidSet knownGtidSet = filteredGtidSet;
        LOGGER.info("Using first available positions for new GTID channels");
        final GtidSet relevantAvailableServerGtidSet =
                (gtidSourceFilter != null)
                        ? availableServerGtidSet.retainAll(gtidSourceFilter)
                        : availableServerGtidSet;
        LOGGER.info("Relevant GTID set available on server: {}", relevantAvailableServerGtidSet);

        GtidSet mergedGtidSet =
                GtidUtils.fixRestoredGtidSet(
                        GtidUtils.mergeGtidSetInto(
                                relevantAvailableServerGtidSet.retainAll(
                                        uuid ->
                                                ((MySqlGtidSet) knownGtidSet).forServerWithId(uuid)
                                                        != null),
                                purgedServerGtidSet),
                        filteredGtidSet);

        LOGGER.info("Final merged GTID set to use when connecting to MySQL: {}", mergedGtidSet);
        return mergedGtidSet;
    }

    @Override
    public boolean isMariaDb() {
        return false;
    }

    @Override
    protected GtidSet createGtidSet(String gtids) {
        return new MySqlGtidSet(gtids);
    }

    @Override
    public String connectionString() {
        return connectionString(urlPattern);
    }

    public static String getJavaEncodingForCharSet(String charSetName) {
        return CharsetMappingWrapper.getJavaEncodingForMysqlCharSet(charSetName);
    }

    /** Helper to gain access to protected method */
    private static final class CharsetMappingWrapper extends CharsetMapping {
        static String getJavaEncodingForMysqlCharSet(String charSetName) {
            return CharsetMapping.getStaticJavaEncodingForMysqlCharset(charSetName);
        }
    }
}