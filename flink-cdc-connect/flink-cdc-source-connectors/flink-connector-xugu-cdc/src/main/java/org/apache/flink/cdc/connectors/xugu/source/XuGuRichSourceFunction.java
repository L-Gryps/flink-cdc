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

package org.apache.flink.cdc.connectors.xugu.source;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.xugu.source.config.XuGuConnectorConfig;
import org.apache.flink.cdc.connectors.xugu.source.connection.XuGuConnection;
import org.apache.flink.cdc.connectors.xugu.source.converter.XuGuValueConverters;
import org.apache.flink.cdc.connectors.xugu.source.offset.XuGuSourceInfo;
import org.apache.flink.cdc.connectors.xugu.source.schema.XuGuDatabaseSchema;
import org.apache.flink.cdc.connectors.xugu.source.schema.XuGuSchema;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;

import com.xugudb.binlog.client.XuGuBinlogClient;
import com.xugudb.binlog.client.api.XuGuEventListener;
import com.xugudb.binlog.client.common.message.LogMessage;
import com.xugudb.binlog.client.common.offset.XuGuOffset;
import com.xugudb.binlog.client.config.XGReadConnectionConfig;
import io.debezium.connector.SnapshotRecord;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.history.TableChanges;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The source implementation for OceanBase that read snapshot events first and then read the change
 * event.
 *
 * @param <T> The type created by the deserializer.
 */
public class XuGuRichSourceFunction<T> extends RichSourceFunction<T>
        implements CheckpointListener, CheckpointedFunction, ResultTypeQueryable<T> {

    private static final long serialVersionUID = 2844054619864617340L;

    private static final Logger LOG = LoggerFactory.getLogger(XuGuRichSourceFunction.class);

    private final StartupOptions startupOptions;
    private final String username;
    private final String password;
    private final String databaseName;
    private final String[] tableList;
    private final String serverTimeZone;
    private final Duration connectTimeout;
    private final String hostname;
    private final Integer port;
    private final String jdbcDriver;
    private final Properties jdbcProperties;
    private final XGReadConnectionConfig xgReadConnectionConfig;
    private final Properties debeziumProperties;
    private final DebeziumDeserializationSchema<T> deserializer;

    private final List<SourceRecord> changeRecordBuffer = new LinkedList<>();

    private transient XuGuConnectorConfig connectorConfig;
    private transient XuGuSourceInfo sourceInfo;
    private transient Set<TableId> tableSet;
    private transient XuGuSchema obSchema;
    private transient XuGuDatabaseSchema databaseSchema;
    private transient volatile long resolvedTimestamp;
    private transient volatile Exception xuguClientException;
    private transient volatile XuGuConnection snapshotConnection;
    private transient XuGuBinlogClient binlogClient;
    private transient ListState<Map<Integer, XuGuOffset>> offsetState;
    private transient OutputCollector<T> outputCollector;

    private transient volatile boolean running;
    private transient java.util.concurrent.BlockingQueue<SourceRecord> emitQueue;
    private transient Map<Integer, XuGuOffset> restoredOffsets; // 从状态恢复的偏移
    private transient Object checkpointLock; // 引用 ctx.getCheckpointLock()

    public XuGuRichSourceFunction(
            StartupOptions startupOptions,
            String username,
            String password,
            String databaseName,
            String[] tableList,
            String serverTimeZone,
            Duration connectTimeout,
            String hostname,
            Integer port,
            String jdbcDriver,
            Properties jdbcProperties,
            XGReadConnectionConfig xgReadConnectionConfig,
            Properties debeziumProperties,
            DebeziumDeserializationSchema<T> deserializer) {
        this.startupOptions = checkNotNull(startupOptions);
        this.username = checkNotNull(username);
        this.password = checkNotNull(password);
        this.databaseName = databaseName;
        this.tableList = tableList;
        this.serverTimeZone = checkNotNull(serverTimeZone);
        this.connectTimeout = checkNotNull(connectTimeout);
        this.hostname = checkNotNull(hostname);
        this.port = checkNotNull(port);
        this.jdbcDriver = checkNotNull(jdbcDriver);
        this.jdbcProperties = jdbcProperties;
        this.xgReadConnectionConfig = xgReadConnectionConfig;
        this.debeziumProperties = debeziumProperties;
        this.deserializer = checkNotNull(deserializer);
    }

    @Override
    public void open(final Configuration config) throws Exception {
        super.open(config);
        this.outputCollector = new OutputCollector<>();
        this.connectorConfig = new XuGuConnectorConfig(serverTimeZone, debeziumProperties);
        this.sourceInfo = new XuGuSourceInfo(connectorConfig);
        this.emitQueue = new java.util.concurrent.ArrayBlockingQueue<>(10_000);
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        this.running = true;
        this.checkpointLock = ctx.getCheckpointLock();
        this.outputCollector.context = ctx;

        LOG.info("Start to initial table whitelist");
        initTableWhiteList();

        if (!startupOptions.isStreamOnly()) {
            sourceInfo.setSnapshot(SnapshotRecord.TRUE);
            long startTimestamp = getSnapshotConnection().getCurrentTimestampSss();
            resolvedTimestamp = startTimestamp;

            synchronized (checkpointLock) {
                readSnapshotRecords();
            }
            sourceInfo.setSnapshot(SnapshotRecord.FALSE);
            LOG.info("Snapshot finished at timestamp {}", startTimestamp);
        }

        if (!startupOptions.isSnapshotOnly()) {
            if (resolvedTimestamp > 0 && xgReadConnectionConfig != null) {
                xgReadConnectionConfig.setStartTimestamp(resolvedTimestamp);
            }
            this.binlogClient = new XuGuBinlogClient(xgReadConnectionConfig);
            if (restoredOffsets != null) {
                binlogClient.restoreOffsets(restoredOffsets);
                LOG.info("Restored offsets: {}", restoredOffsets);
            }
            // 注册监听器
            binlogClient.addListener(
                    new XuGuEventListener() {
                        @Override
                        public void onEvent(LogMessage message) {
                            try {
                                switch (message.getAction()) {
                                    case INSERT:
                                    case UPDATE:
                                    case DELETE:
                                        {
                                            SourceRecord r = getChangeRecord(message);
                                            if (r != null) {
                                                emitQueue.put(r);
                                            }
                                            break;
                                        }
                                    default:
                                        { // DDL
                                            try {
                                                // 先刷新 schema（确保后续 DML 使用新 schema）
                                                TableId tid =
                                                        tableId(
                                                                message.getSchemaName(),
                                                                message.getTableName());
                                                if (tableSet.contains(tid)) {
                                                    refreshTableSchema(tid);
                                                }
                                                // 如需向下游发出 DDL 事件，可在此构造自定义 SourceRecord 放入 emitQueue
                                            } catch (Throwable t) {
                                                onError(
                                                        new FlinkRuntimeException(
                                                                "DDL handling failed", t));
                                            }
                                        }
                                }
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                            } catch (Throwable t) {
                                onError(new FlinkRuntimeException("Listener failed", t));
                            }
                        }

                        @Override
                        public void onError(Exception e) {
                            xuguClientException = e;
                            try {
                                binlogClient.stop();
                            } catch (Throwable ignore) {
                            }
                        }

                        @Override
                        public Map<Integer, XuGuOffset> getCurrentOffsets() {
                            return binlogClient.getCurrentOffsets();
                        }
                    });

            sourceInfo.setSnapshot(SnapshotRecord.FALSE);
            LOG.info("Change events reading started");
            while (running) {
                if (xuguClientException != null) {
                    throw new FlinkRuntimeException("Binlog client error", xuguClientException);
                }
                SourceRecord r = emitQueue.poll(1, TimeUnit.SECONDS);
                if (r != null) {
                    synchronized (checkpointLock) {
                        deserializer.deserialize(r, outputCollector);
                    }
                }
            }
        }
    }

    private XuGuConnection getSnapshotConnection() {
        if (snapshotConnection == null) {
            snapshotConnection =
                    new XuGuConnection(
                            hostname,
                            port,
                            username,
                            password,
                            databaseName,
                            connectTimeout,
                            jdbcDriver,
                            jdbcProperties,
                            getClass().getClassLoader());
        }
        return snapshotConnection;
    }

    private void closeSnapshotConnection() {
        if (snapshotConnection != null) {
            try {
                snapshotConnection.close();
            } catch (SQLException e) {
                LOG.error("Failed to close snapshotConnection", e);
            }
            snapshotConnection = null;
        }
    }

    private TableId tableId(String schemaName, String tableName) {
        return new TableId(null, schemaName, tableName);
    }

    private void initTableWhiteList() {
        if (tableSet != null && !tableSet.isEmpty()) {
            return;
        }

        final Set<TableId> localTableSet = new HashSet<>();

        for (String table : tableList) {
            if (StringUtils.isNotBlank(table)) {
                TableId tableId = TableId.parse(table, false);
                localTableSet.add(tableId);
            }
        }

        if (localTableSet.isEmpty()) {
            throw new FlinkRuntimeException("No valid table found");
        }

        LOG.info("Table list: {}", localTableSet);
        this.tableSet = localTableSet;
        // for some 4.x versions, it will be treated as 'tenant.*.*'
        if (this.xgReadConnectionConfig != null) {
            this.xgReadConnectionConfig.setIncludeTables(
                    localTableSet.stream()
                            .map(tableId -> tableId.toString())
                            .collect(Collectors.toList()));
        }
        LOG.info("Table list: {}", localTableSet);
    }

    private TableSchema getTableSchema(TableId tableId) {
        if (databaseSchema == null) {
            databaseSchema =
                    new XuGuDatabaseSchema(
                            connectorConfig,
                            new XuGuValueConverters(connectorConfig),
                            t -> tableSet.contains(t),
                            false);
        }
        TableSchema tableSchema = databaseSchema.schemaFor(tableId);
        if (tableSchema != null) {
            return tableSchema;
        }

        if (obSchema == null) {
            obSchema = new XuGuSchema();
        }
        TableChanges.TableChange tableChange =
                obSchema.getTableSchema(getSnapshotConnection(), tableId);
        databaseSchema.refresh(tableChange.getTable());
        return databaseSchema.schemaFor(tableId);
    }

    protected void readSnapshotRecords() {
        tableSet.forEach(this::readSnapshotRecordsByTable);
    }

    private void readSnapshotRecordsByTable(TableId tableId) {
        String fullName = getSnapshotConnection().quotedTableIdString(tableId);
        sourceInfo.tableEvent(tableId);
        try (XuGuConnection connection = getSnapshotConnection()) {
            LOG.info("Start to read snapshot from {}", connection.quotedTableIdString(tableId));
            connection.query(
                    "SELECT * FROM " + fullName,
                    rs -> {
                        TableSchema tableSchema = getTableSchema(tableId);
                        List<Field> fields = tableSchema.valueSchema().fields();

                        while (rs.next()) {
                            Object[] fieldValues = new Object[fields.size()];
                            for (Field field : fields) {
                                fieldValues[field.index()] = rs.getObject(field.name());
                            }
                            Struct value = tableSchema.valueFromColumnData(fieldValues);
                            Instant now = Instant.now();
                            Struct struct =
                                    tableSchema
                                            .getEnvelopeSchema()
                                            .read(value, sourceInfo.struct(), now);
                            synchronized (checkpointLock) {
                                try {
                                    deserializer.deserialize(
                                            new SourceRecord(
                                                    null,
                                                    null,
                                                    tableId.identifier(),
                                                    null,
                                                    null,
                                                    null,
                                                    struct.schema(),
                                                    struct),
                                            outputCollector);
                                } catch (Exception e) {
                                    LOG.error("Deserialize snapshot record failed ", e);
                                    throw new FlinkRuntimeException(e);
                                }
                            }
                        }
                    });
            LOG.info("Read snapshot from {} finished", fullName);
        } catch (SQLException e) {
            LOG.error("Read snapshot from table " + fullName + " failed", e);
            throw new FlinkRuntimeException(e);
        }
    }

    protected void readChangeRecords() throws InterruptedException, TimeoutException {
        if (resolvedTimestamp > 0) {
            xgReadConnectionConfig.setStartTimestamp(resolvedTimestamp);
            LOG.info("Restore from timestamp: {}", resolvedTimestamp);
        }

        this.binlogClient = new XuGuBinlogClient(xgReadConnectionConfig);

        final CountDownLatch latch = new CountDownLatch(1);

        binlogClient.addListener(
                new XuGuEventListener() {

                    @Override
                    public void onEvent(LogMessage message) {
                        switch (message.getAction()) {
                            case INSERT:
                            case UPDATE:
                            case DELETE:
                                SourceRecord record = getChangeRecord(message);
                                if (record != null) {
                                    changeRecordBuffer.add(record);
                                }
                                changeRecordBuffer.forEach(
                                        r -> {
                                            try {
                                                deserializer.deserialize(r, outputCollector);
                                            } catch (Exception e) {
                                                throw new FlinkRuntimeException(e);
                                            }
                                        });
                                changeRecordBuffer.clear();
                                break;
                            default:
                                LOG.trace(
                                        "Ddl: {}",
                                        message.getFields().get(0).getValue().toString());
                                break;
                        }
                    }

                    @Override
                    public void onError(Exception e) {
                        xuguClientException = e;
                        binlogClient.stop();
                    }

                    @Override
                    public Map<Integer, XuGuOffset> getCurrentOffsets() {
                        return binlogClient.getCurrentOffsets();
                    }
                });

        LOG.info("Try to start XGBinlogClient withconfig: {}", xgReadConnectionConfig);

        if (!latch.await(connectTimeout.getSeconds(), TimeUnit.SECONDS)) {
            throw new TimeoutException(
                    "Timeout to receive log messages in XGBinlogClient.RecordListener");
        }
        LOG.info("XGBinlogClient started successfully");
    }

    private SourceRecord getChangeRecord(LogMessage message) {
        TableId tableId = tableId(message.getSchemaName(), message.getTableName());
        if (!tableSet.contains(tableId)) {
            return null;
        }

        sourceInfo.tableEvent(tableId);
        sourceInfo.setSourceTime(message.getCommitTime());
        sourceInfo.setCurrentFileNum(message.getFno());
        sourceInfo.setCurrentFilePosition(message.getFpos());
        sourceInfo.setCurrentPartitionId(message.getPartitionId());
        sourceInfo.setTransactionId(message.getTransactionId());
        Struct source = sourceInfo.struct();

        TableSchema tableSchema = getTableSchema(tableId);
        Struct struct;
        Schema valueSchema = tableSchema.valueSchema();
        List<Field> fields = valueSchema.fields();
        Struct before, after;
        Object[] beforeFieldValues, afterFieldValues;
        Map<String, Object> beforeValueMap = new HashMap<>();
        Map<String, Object> afterValueMap = new HashMap<>();
        message.getFields()
                .forEach(
                        field -> {
                            if (field.isPrev()) {
                                beforeValueMap.put(field.getName(), getFieldValue(field));
                            } else {
                                afterValueMap.put(field.getName(), getFieldValue(field));
                            }
                        });
        switch (message.getAction()) {
            case INSERT:
                afterFieldValues = new Object[fields.size()];
                for (Field field : fields) {
                    afterFieldValues[field.index()] = afterValueMap.get(field.name());
                }
                after = tableSchema.valueFromColumnData(afterFieldValues);
                struct = tableSchema.getEnvelopeSchema().create(after, source, Instant.now());
                break;
            case DELETE:
                beforeFieldValues = new Object[fields.size()];
                for (Field field : fields) {
                    beforeFieldValues[field.index()] = beforeValueMap.get(field.name());
                }
                before = tableSchema.valueFromColumnData(beforeFieldValues);
                struct = tableSchema.getEnvelopeSchema().delete(before, source, Instant.now());
                break;
            case UPDATE:
                beforeFieldValues = new Object[fields.size()];
                afterFieldValues = new Object[fields.size()];
                for (Field field : fields) {
                    beforeFieldValues[field.index()] = beforeValueMap.get(field.name());
                    afterFieldValues[field.index()] = afterValueMap.get(field.name());
                }
                before = tableSchema.valueFromColumnData(beforeFieldValues);
                after = tableSchema.valueFromColumnData(afterFieldValues);
                struct =
                        tableSchema
                                .getEnvelopeSchema()
                                .update(before, after, source, Instant.now());
                break;
            default:
                throw new UnsupportedOperationException();
        }
        return new SourceRecord(
                null, null, tableId.identifier(), null, null, null, struct.schema(), struct);
    }

    private Object getFieldValue(LogMessage.Field field) {
        if (field.getValue() == null) {
            return null;
        }
        return field.getValue();
    }

    private void refreshTableSchema(TableId tableId) {
        if (databaseSchema == null) {
            databaseSchema =
                    new XuGuDatabaseSchema(
                            connectorConfig,
                            new XuGuValueConverters(connectorConfig),
                            t -> tableSet.contains(t),
                            false);
        }
        if (obSchema == null) {
            obSchema = new XuGuSchema();
        }
        // 直接从数据库读取最新表结构并刷新到 databaseSchema
        TableChanges.TableChange tc = obSchema.getTableSchema(getSnapshotConnection(), tableId);
        databaseSchema.refresh(tc.getTable());
        LOG.info("Refreshed schema for table {} due to DDL", tableId);
    }

    @Override
    public void notifyCheckpointComplete(long l) {
        // do nothing
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return this.deserializer.getProducedType();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

        Map<Integer, XuGuOffset> current =
                binlogClient != null ? binlogClient.getCurrentOffsets() : null;
        LOG.info(
                "snapshotState checkpoint: {} at xuguOffset: {}",
                context.getCheckpointId(),
                current);
        offsetState.clear();
        if (current != null) {
            offsetState.add(current);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        LOG.info("initialize checkpoint");

        offsetState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "xugudb-offsets",
                                        TypeInformation.of(
                                                new TypeHint<Map<Integer, XuGuOffset>>() {})));

        if (context.isRestored()) {
            for (Map<Integer, XuGuOffset> offsets : offsetState.get()) {
                if (binlogClient != null) {
                    binlogClient.restoreOffsets(offsets);
                }
                break;
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
        closeSnapshotConnection();
        if (binlogClient != null) {
            try {
                binlogClient.stop();
            } catch (Throwable e) {
                LOG.warn("Stop binlog client error", e);
            }
            binlogClient = null;
        }
    }

    private static class OutputCollector<T> implements Collector<T> {

        private SourceContext<T> context;

        @Override
        public void collect(T record) {
            context.collect(record);
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
