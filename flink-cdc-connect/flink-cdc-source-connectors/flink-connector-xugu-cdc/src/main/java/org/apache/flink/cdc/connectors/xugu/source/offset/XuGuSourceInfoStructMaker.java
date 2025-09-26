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

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.SourceInfoStructMaker;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.time.Instant;

/** The {@link SourceInfoStructMaker} implementation for OceanBase. */
public class XuGuSourceInfoStructMaker implements SourceInfoStructMaker<XuGuSourceInfo> {
    private final Schema schema;

    public XuGuSourceInfoStructMaker() {
        this.schema =
                SchemaBuilder.struct()
                        .field(XuGuSourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                        .field(XuGuSourceInfo.TIMESTAMP_KEY, Schema.INT64_SCHEMA)
                        .field(XuGuSourceInfo.BINLOG_PARTITION_ID_KEY, Schema.INT32_SCHEMA)
                        .field(XuGuSourceInfo.BINLOG_FILE_NO_KEY, Schema.INT32_SCHEMA)
                        .field(XuGuSourceInfo.BINLOG_FILE_POSITION_KEY, Schema.INT64_SCHEMA)
                        .field(XuGuSourceInfo.DATABASE_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                        .field(XuGuSourceInfo.SCHEMA_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                        .field(XuGuSourceInfo.TRANSACTION_ID_KEY, Schema.INT64_SCHEMA)
                        .build();
    }

    @Override
    public void init(String connector, String version, CommonConnectorConfig connectorConfig) {}

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(XuGuSourceInfo sourceInfo) {
        Struct source = new Struct(schema);
        source.put(XuGuSourceInfo.TABLE_NAME_KEY, sourceInfo.table());

        Instant timestamp = sourceInfo.timestamp();
        source.put(XuGuSourceInfo.TIMESTAMP_KEY, timestamp != null ? timestamp.toEpochMilli() : 0);

        source.put(
                XuGuSourceInfo.BINLOG_PARTITION_ID_KEY,
                sourceInfo.getCurrentPartitionId() != null
                        ? sourceInfo.getCurrentPartitionId()
                        : 0);
        source.put(
                XuGuSourceInfo.BINLOG_FILE_NO_KEY,
                sourceInfo.getCurrentFileNum() != null ? sourceInfo.getCurrentFileNum() : 0);
        source.put(
                XuGuSourceInfo.BINLOG_FILE_POSITION_KEY,
                sourceInfo.getCurrentFilePosition() != null
                        ? sourceInfo.getCurrentFilePosition()
                        : 0);

        if (sourceInfo.database() != null) {
            source.put(XuGuSourceInfo.DATABASE_NAME_KEY, sourceInfo.database());
        }
        if (sourceInfo.tableSchema() != null) {
            source.put(XuGuSourceInfo.SCHEMA_NAME_KEY, sourceInfo.tableSchema());
        }

        source.put(
                XuGuSourceInfo.TRANSACTION_ID_KEY,
                sourceInfo.getTransactionId() != null ? sourceInfo.getTransactionId() : 0);

        return source;
    }
}
