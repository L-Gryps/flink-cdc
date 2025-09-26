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

package org.apache.flink.cdc.connectors.xugu.source.offset;

import org.apache.flink.cdc.connectors.xugu.source.config.XuGuConnectorConfig;

import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.relational.TableId;

import java.time.Instant;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** OceanBase source info. */
public class XuGuSourceInfo extends BaseSourceInfo {

    public static final String BINLOG_PARTITION_ID_KEY = "partition_id";
    public static final String BINLOG_FILE_NO_KEY = "fno";
    public static final String BINLOG_FILE_POSITION_KEY = "fpos";
    public static final String TRANSACTION_ID_KEY = "transaction_id";

    private final String dbName;

    private Integer currentPartitionId = 0;
    private Integer currentFileNum = 0;
    private Long currentFilePosition = 0L;
    private Instant sourceTime;
    private Set<TableId> tableIds;
    private Long transactionId = 0L;

    public XuGuSourceInfo(XuGuConnectorConfig config) {
        super(config);
        this.dbName = config.databaseName();
    }

    @Override
    protected Instant timestamp() {
        return sourceTime;
    }

    public void setSourceTime(Instant sourceTime) {
        this.sourceTime = sourceTime;
    }

    public Integer getCurrentPartitionId() {
        return currentPartitionId;
    }

    public void setCurrentPartitionId(Integer currentPartitionId) {
        this.currentPartitionId = currentPartitionId;
    }

    public Integer getCurrentFileNum() {
        return currentFileNum;
    }

    public void setCurrentFileNum(Integer currentFileNum) {
        this.currentFileNum = currentFileNum;
    }

    public Long getCurrentFilePosition() {
        return currentFilePosition;
    }

    public void setCurrentFilePosition(Long currentFilePosition) {
        this.currentFilePosition = currentFilePosition;
    }

    public Long getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(Long transactionId) {
        this.transactionId = transactionId;
    }

    public void tableEvent(TableId tableId) {
        this.tableIds = Collections.singleton(tableId);
    }

    @Override
    protected String database() {
        return dbName;
    }

    public String tableSchema() {
        return (tableIds == null || tableIds.isEmpty())
                ? null
                : tableIds.stream()
                        .filter(Objects::nonNull)
                        .map(TableId::schema)
                        .filter(Objects::nonNull)
                        .distinct()
                        .collect(Collectors.joining(","));
    }

    public String table() {
        return (tableIds == null || tableIds.isEmpty())
                ? null
                : tableIds.stream()
                        .filter(Objects::nonNull)
                        .map(TableId::table)
                        .collect(Collectors.joining(","));
    }
}
