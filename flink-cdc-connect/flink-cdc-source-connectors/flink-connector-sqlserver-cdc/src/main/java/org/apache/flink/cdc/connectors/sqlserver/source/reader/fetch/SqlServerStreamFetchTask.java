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

package org.apache.flink.cdc.connectors.sqlserver.source.reader.fetch;

import org.apache.flink.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.sqlserver.source.offset.LsnOffset;

import io.debezium.DebeziumException;
import io.debezium.connector.sqlserver.Lsn;
import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.connector.sqlserver.SqlServerConnectorTask;
import io.debezium.connector.sqlserver.SqlServerDatabaseSchema;
import io.debezium.connector.sqlserver.SqlServerOffsetContext;
import io.debezium.connector.sqlserver.SqlServerPartition;
import io.debezium.connector.sqlserver.SqlServerStreamingChangeEventSource;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.schema.SchemaFactory;
import io.debezium.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.cdc.connectors.sqlserver.source.offset.LsnOffset.NO_STOPPING_OFFSET;

/** The task to work for fetching data of SqlServer table stream split . */
public class SqlServerStreamFetchTask implements FetchTask<SourceSplitBase> {

    private final StreamSplit split;
    private volatile boolean taskRunning = false;

    public SqlServerStreamFetchTask(StreamSplit split) {
        this.split = split;
    }

    @Override
    public void execute(Context context) throws Exception {
        SqlServerSourceFetchTaskContext sourceFetchContext =
                (SqlServerSourceFetchTaskContext) context;
        sourceFetchContext.getOffsetContext().preSnapshotCompletion();
        taskRunning = true;
        NotificationService<SqlServerPartition, SqlServerOffsetContext> notificationService =
                new NotificationService<>(
                        new SqlServerConnectorTask().getNotificationChannels(),
                        sourceFetchContext.getDbzConnectorConfig(),
                        SchemaFactory.get(),
                        (record) -> {
                            sourceFetchContext.getQueue().enqueue(new DataChangeEvent(record));
                        });
        StreamSplitReadTask redoLogSplitReadTask =
                new StreamSplitReadTask(
                        sourceFetchContext.getDbzConnectorConfig(),
                        sourceFetchContext.getConnection(),
                        sourceFetchContext.getMetaDataConnection(),
                        sourceFetchContext.getDispatcher(),
                        sourceFetchContext.getErrorHandler(),
                        sourceFetchContext.getDatabaseSchema(),
                        split,
                        notificationService);
        RedoLogSplitChangeEventSourceContext changeEventSourceContext =
                new RedoLogSplitChangeEventSourceContext();
        redoLogSplitReadTask.init(sourceFetchContext.getOffsetContext());
        redoLogSplitReadTask.execute(
                changeEventSourceContext,
                sourceFetchContext.getPartition(),
                sourceFetchContext.getOffsetContext());
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public StreamSplit getSplit() {
        return split;
    }

    @Override
    public void close() {
        taskRunning = false;
    }

    /**
     * A wrapped task to read all binlog for table and also supports read bounded (from lowWatermark
     * to highWatermark) binlog.
     */
    public static class StreamSplitReadTask extends SqlServerStreamingChangeEventSource {

        private static final Logger LOG = LoggerFactory.getLogger(StreamSplitReadTask.class);
        private final StreamSplit lsnSplit;
        private final JdbcSourceEventDispatcher<SqlServerPartition> dispatcher;
        private final ErrorHandler errorHandler;
        private ChangeEventSourceContext context;

        public StreamSplitReadTask(
                SqlServerConnectorConfig connectorConfig,
                SqlServerConnection connection,
                SqlServerConnection metadataConnection,
                JdbcSourceEventDispatcher<SqlServerPartition> dispatcher,
                ErrorHandler errorHandler,
                SqlServerDatabaseSchema schema,
                StreamSplit lsnSplit,
                NotificationService<SqlServerPartition, SqlServerOffsetContext>
                        notificationService) {
            super(
                    connectorConfig,
                    connection,
                    metadataConnection,
                    dispatcher,
                    errorHandler,
                    Clock.system(),
                    schema,
                    notificationService);
            this.lsnSplit = lsnSplit;
            this.dispatcher = dispatcher;
            this.errorHandler = errorHandler;
        }

        @Override
        public void afterHandleLsn(SqlServerPartition partition, Lsn toLsn) {
            // check do we need to stop for fetch binlog for snapshot split.
            if (isBoundedRead()) {
                LsnOffset currentLsnOffset = new LsnOffset(null, toLsn, null);
                Offset endingOffset = lsnSplit.getEndingOffset();
                if (currentLsnOffset.isAtOrAfter(endingOffset)) {
                    // send streaming end event
                    try {
                        dispatcher.dispatchWatermarkEvent(
                                partition.getSourcePartition(),
                                lsnSplit,
                                currentLsnOffset,
                                WatermarkKind.END);
                    } catch (InterruptedException e) {
                        LOG.error("Send signal event error.", e);
                        errorHandler.setProducerThrowable(
                                new DebeziumException("Error processing binlog signal event", e));
                    }
                    // tell fetcher the streaming task finished
                    ((SqlServerScanFetchTask.SqlserverSnapshotSplitChangeEventSourceContext)
                                    context)
                            .finished();
                }
            }
        }

        private boolean isBoundedRead() {
            return !NO_STOPPING_OFFSET.equals(lsnSplit.getEndingOffset());
        }

        @Override
        public void execute(
                ChangeEventSourceContext context,
                SqlServerPartition partition,
                SqlServerOffsetContext offsetContext)
                throws InterruptedException {
            this.context = context;
            super.execute(context, partition, offsetContext);
        }
    }

    /**
     * The {@link ChangeEventSource.ChangeEventSourceContext} implementation for binlog split task.
     */
    private class RedoLogSplitChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {
        @Override
        public boolean isPaused() {
            return false;
        }

        @Override
        public boolean isRunning() {
            return taskRunning;
        }

        @Override
        public void resumeStreaming() throws InterruptedException {}

        @Override
        public void waitSnapshotCompletion() throws InterruptedException {}

        @Override
        public void streamingPaused() {}

        @Override
        public void waitStreamingPaused() throws InterruptedException {}
    }
}
