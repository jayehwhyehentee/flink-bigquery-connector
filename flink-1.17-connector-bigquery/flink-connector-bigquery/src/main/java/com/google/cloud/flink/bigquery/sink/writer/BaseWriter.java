/*
 * Copyright (C) 2024 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.flink.bigquery.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.StringUtils;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.bigquery.storage.v1.ProtoSchemaConverter;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.exceptions.BigQueryConnectorException;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.BigQueryServicesFactory;
import com.google.cloud.flink.bigquery.sink.client.BigQueryClientWithErrorHandling;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.cloud.flink.bigquery.sink.serializer.BigQueryProtoSerializer;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProvider;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProviderImpl;
import com.google.cloud.flink.bigquery.sink.throttle.BigQueryWriterThrottler;
import com.google.cloud.flink.bigquery.sink.throttle.Throttler;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import static com.google.cloud.flink.bigquery.common.utils.AvroToBigQuerySchemaTransform.getBigQuerySchema;

/**
 * Base class for developing a BigQuery writer.
 *
 * <p>This class abstracts implementation details common for BigQuery writers which use the {@link
 * StreamWriter}.
 *
 * <p>Key methods to note are "write" (implemented in child classes) and "flush" (implemented here).
 * Write gets called for every record and flush gets invoked at checkpoints. Writes collect records
 * to maximize the payload of BigQuery storage write APIs. Flush method is used to send pending
 * records before checkpoint completion.
 *
 * <p>BigQuery write APIs are invoked asynchronously for better performance, and responses are
 * validated lazily.
 *
 * <p>Serializer's "init" method is called in the writer's constructor because the resulting {@link
 * Descriptor} is not serializable and cannot be propagated to machines hosting writer instances.
 * Hence, this derivation of descriptors must be performed during writer initialization.
 *
 * @param <IN> Type of records to be written to BigQuery.
 */
abstract class BaseWriter<IN> implements SinkWriter<IN> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    // Multiply 0.95 to keep a buffer from exceeding payload limits.
    private static final long MAX_APPEND_REQUEST_BYTES =
            (long) (StreamWriter.getApiMaxRequestBytes() * 0.95);

    // Number of bytes to be sent in the next append request.
    private long appendRequestSizeBytes;
    protected final int subtaskId;
    private final String tablePath;
    private final BigQueryConnectOptions connectOptions;
    private final BigQueryProtoSerializer<IN> serializer;
    private final ProtoRows.Builder protoRowsBuilder;
    // Writer must be throttled to ensure proper client usage.
    private final Throttler throttler;

    private BigQuerySchemaProvider schemaProvider;
    private ProtoSchema protoSchema;
    private boolean firstWritePostConstructor = true;

    final CreateTableOptions createTableOptions;
    final boolean fatalizeSerializer;
    final String traceId;
    final Queue<AppendInfo> appendResponseFuturesQueue;
    // Initialization of writeClient has been deferred to first append call. BigQuery's best
    // practices suggest that client connections should be opened when needed.
    BigQueryServices.StorageWriteClient writeClient;
    StreamWriter streamWriter;
    String streamName;
    long totalRecordsSeen;
    // In at-least-once mode, "totalRecordsWritten" represents records written to BigQuery table.
    // In exactly-once mode, "totalRecordsWritten" actually represents records appended to a
    // write stream by this writer. Only at a checkpoint, when sink's commit is invoked, will
    // the records in a stream get committed to the table. Hence, records written to BigQuery
    // table is equal to this "totalRecordsWritten" only upon checkpoint completion.
    long totalRecordsWritten;
    // Counters for metric reporting
    Counter numberOfRecordsWrittenToBigQuery;
    Counter numberOfRecordsSeenByWriter;
    Counter numberOfRecordsSeenByWriterSinceCheckpoint;

    BaseWriter(
            int subtaskId,
            String tablePath,
            BigQueryConnectOptions connectOptions,
            BigQuerySchemaProvider schemaProvider,
            BigQueryProtoSerializer serializer,
            CreateTableOptions createTableOptions,
            boolean fatalizeSerializer,
            int maxParallelism,
            String traceId) {
        this.subtaskId = subtaskId;
        this.tablePath = tablePath;
        this.connectOptions = connectOptions;
        this.schemaProvider = schemaProvider;
        this.serializer = serializer;
        this.createTableOptions = createTableOptions;
        this.fatalizeSerializer = fatalizeSerializer;
        this.traceId = traceId;
        appendRequestSizeBytes = 0L;
        appendResponseFuturesQueue = new LinkedList<>();
        protoRowsBuilder = ProtoRows.newBuilder();
        throttler = new BigQueryWriterThrottler(subtaskId, maxParallelism);
    }

    /** Append pending records and validate all remaining append responses. */
    @Override
    public void flush(boolean endOfInput) {
        if (appendRequestSizeBytes > 0) {
            append();
        }
        logger.info("Validating all pending append responses in subtask {}", subtaskId);
        validateAppendResponses(true);
    }

    /** Close resources maintained by this writer. */
    @Override
    public void close() {
        logger.debug("Closing writer in subtask {}", subtaskId);
        if (protoRowsBuilder != null) {
            protoRowsBuilder.clear();
        }
        if (appendResponseFuturesQueue != null) {
            appendResponseFuturesQueue.clear();
        }
        if (streamWriter != null) {
            streamWriter.close();
        }
        if (writeClient != null) {
            writeClient.close();
        }
    }

    /** Invoke BigQuery storage API for appending data to a table. */
    abstract void sendAppendRequest(ProtoRows protoRows);

    /** Checks append response for errors. */
    abstract void validateAppendResponse(AppendInfo appendInfo);

    void preWrite(IN element) {
        if (firstWritePostConstructor) {
            ensureSchemaAwareness(element);
            firstWritePostConstructor = false;
        }
        totalRecordsSeen++;
        numberOfRecordsSeenByWriter.inc();
        numberOfRecordsSeenByWriterSinceCheckpoint.inc();
    }

    void ensureSchemaAwareness(IN record) {
        if (schemaProvider.schemaUnknown()) {
            // Try getting destination table schema from BigQuery table
            schemaProvider = new BigQuerySchemaProviderImpl(connectOptions);
            if (schemaProvider.schemaUnknown()) {
                // Derive schema from the record
                Schema avroSchema = serializer.getAvroSchema(record);
                // Create new schema provider with known schema
                schemaProvider = new BigQuerySchemaProviderImpl(avroSchema);
            }
        }
        protoSchema = getProtoSchema(schemaProvider);
        serializer.init(schemaProvider);
    }

    /** Add serialized record to append request. */
    void addToAppendRequest(ByteString protoRow) {
        protoRowsBuilder.addSerializedRows(protoRow);
        appendRequestSizeBytes += getProtoRowBytes(protoRow);
    }

    /** Send append request to BigQuery storage and prepare for next append request. */
    void append() {
        // Before sending data to BigQuery, check if new table needs to be created.
        if (totalRecordsWritten == 0) {
            // Throttle writer to ensure proper usage of BigQuery APIs.
            logger.info("Throttling BigQuery writer in subtask {}", subtaskId);
            throttler.throttle();
            // Verify if destination table exists. If not, then create one based on sink config.
            ensureTableExists();
        }
        sendAppendRequest(protoRowsBuilder.build());
        protoRowsBuilder.clear();
        appendRequestSizeBytes = 0L;
    }

    void ensureTableExists() {
        if (BigQueryClientWithErrorHandling.tableExists(connectOptions)) {
            return;
        }
        if (!createTableOptions.enableTableCreation()) {
            logger.error(
                    "Enable table creation flag in BigQuerySinkConfig is destination BigQuery table doesn't already exist");
            throw new IllegalStateException(
                    "Destination BigQuery table does not exist and table creation is not enabled in sink config");
        }
        logger.debug(
                "Attempting to create BigQuery dataset {}.{}",
                connectOptions.getProjectId(),
                connectOptions.getDataset());
        BigQueryClientWithErrorHandling.createDataset(
                connectOptions, createTableOptions.getRegion());
        logger.debug(
                "Attempting to create BigQuery table {}.{}.{}",
                connectOptions.getProjectId(),
                connectOptions.getDataset(),
                connectOptions.getTable());
        BigQueryClientWithErrorHandling.createTable(connectOptions, getTableDefinition());
    }

    /** Creates a StreamWriter for appending to BigQuery table. */
    void createStreamWriter(boolean enableConnectionPool) {
        try {
            if (writeClient == null) {
                writeClient = BigQueryServicesFactory.instance(connectOptions).storageWrite();
            }
            logger.info(
                    "Creating BigQuery StreamWriter for write stream {} in subtask {}",
                    streamName,
                    subtaskId);
            streamWriter =
                    writeClient.createStreamWriter(
                            streamName, protoSchema, enableConnectionPool, traceId);
        } catch (IOException e) {
            logger.error(
                    String.format(
                            "Unable to create StreamWriter for stream %s in subtask %d",
                            streamName, subtaskId),
                    e);
            throw new BigQueryConnectorException("Unable to connect to BigQuery", e);
        }
    }

    /** Creates a write stream for appending to BigQuery table. */
    void createWriteStream(WriteStream.Type streamType) {
        try {
            if (writeClient == null) {
                writeClient = BigQueryServicesFactory.instance(connectOptions).storageWrite();
            }
            logger.info("Creating BigQuery write stream in subtask {}", subtaskId);
            streamName = writeClient.createWriteStream(tablePath, streamType).getName();
        } catch (IOException e) {
            logger.error(
                    String.format("Unable to create write stream in subtask %d", subtaskId), e);
            throw new BigQueryConnectorException("Unable to connect to BigQuery", e);
        }
    }

    /** Checks if serialized record can fit in current append request. */
    boolean fitsInAppendRequest(ByteString protoRow) {
        return appendRequestSizeBytes + getProtoRowBytes(protoRow) <= MAX_APPEND_REQUEST_BYTES;
    }

    /**
     * Serializes a record to BigQuery's proto format.
     *
     * @param element Record to serialize.
     * @return ByteString.
     * @throws BigQuerySerializationException If serialization to proto format failed.
     */
    ByteString getProtoRow(IN element) throws BigQuerySerializationException {
        ByteString protoRow = serializer.serialize(element);
        if (getProtoRowBytes(protoRow) > MAX_APPEND_REQUEST_BYTES) {
            logger.error(
                    "A single row of size %d bytes exceeded the allowed maximum of %d bytes for an append request",
                    getProtoRowBytes(protoRow), MAX_APPEND_REQUEST_BYTES);
            throw new BigQuerySerializationException(
                    "Record size exceeds BigQuery append request limit");
        }
        return protoRow;
    }

    /** Computes {@link ProtoSchema} for BigQuery table. */
    private static ProtoSchema getProtoSchema(BigQuerySchemaProvider schemaProvider) {
        return ProtoSchemaConverter.convert(schemaProvider.getDescriptor());
    }

    /** Gets size of serialized proto row. */
    private int getProtoRowBytes(ByteString protoRow) {
        // Protobuf overhead is at least 2 bytes per row.
        return protoRow.size() + 2;
    }

    /**
     * Throws a RuntimeException if an error is found in appends thus far. Since responses arrive in
     * order, we proceed to check the next response only after the previous one has arrived.
     */
    void validateAppendResponses(boolean waitForResponse) {
        while (!appendResponseFuturesQueue.isEmpty()) {
            AppendInfo appendInfo = appendResponseFuturesQueue.peek();
            if (waitForResponse || appendInfo.getFuture().isDone()) {
                appendResponseFuturesQueue.poll();
                validateAppendResponse(appendInfo);
            } else {
                break;
            }
        }
    }

    void logAndThrowFatalException(Throwable error) {
        logger.error(String.format("AppendRows request failed in subtask %d", subtaskId), error);
        throw new BigQueryConnectorException("Error while writing to BigQuery", error);
    }

    void logAndThrowFatalException(String errorMessage) {
        logger.error(
                String.format(
                        "AppendRows request failed in subtask %d\n%s", subtaskId, errorMessage));
        throw new BigQueryConnectorException(
                String.format("Error while writing to BigQuery\n%s", errorMessage));
    }

    /**
     * Method to initialize flink metrics which are common across at-least-once and exactly-once
     * approach.
     *
     * @param sinkWriterMetricGroup Metric Group to register the counters.
     */
    void initializeMetrics(SinkWriterMetricGroup sinkWriterMetricGroup) {
        numberOfRecordsSeenByWriter = sinkWriterMetricGroup.counter("numberOfRecordsSeenByWriter");
        // Count of records which are successfully appended to BQ.
        numberOfRecordsWrittenToBigQuery =
                sinkWriterMetricGroup.counter("numberOfRecordsWrittenToBigQuery");
        numberOfRecordsSeenByWriterSinceCheckpoint =
                sinkWriterMetricGroup.counter("numberOfRecordsSeenByWriterSinceCheckpoint");
    }

    TableDefinition getTableDefinition() {
        StandardTableDefinition.Builder tableDefinitionBuilder =
                StandardTableDefinition.newBuilder();
        if (createTableOptions.getPartitionType() != null) {
            // Set partitioning
            TimePartitioning.Builder partitioningBuilder =
                    TimePartitioning.newBuilder(createTableOptions.getPartitionType());
            if (!StringUtils.isNullOrWhitespaceOnly(createTableOptions.getPartitionField())) {
                partitioningBuilder.setField(createTableOptions.getPartitionField());
            }
            if (createTableOptions.getPartitionExpirationMillis() > 0) {
                partitioningBuilder.setExpirationMs(
                        createTableOptions.getPartitionExpirationMillis());
            }
            tableDefinitionBuilder.setTimePartitioning(partitioningBuilder.build());
        }
        if (createTableOptions.getClusteredFields() != null
                && !createTableOptions.getClusteredFields().isEmpty()) {
            // Set clustering
            Clustering clustering =
                    Clustering.newBuilder()
                            .setFields(createTableOptions.getClusteredFields())
                            .build();
            tableDefinitionBuilder.setClustering(clustering);
        }
        // Set BigQuery table schema
        tableDefinitionBuilder.setSchema(getBigQuerySchema(schemaProvider.getAvroSchema()));
        return tableDefinitionBuilder.build();
    }

    static class AppendInfo {
        private final ApiFuture<AppendRowsResponse> future;
        private final long expectedOffset;
        private final long recordsAppended;

        AppendInfo(
                ApiFuture<AppendRowsResponse> future, long expectedOffset, long recordsAppended) {
            this.future = future;
            this.expectedOffset = expectedOffset;
            this.recordsAppended = recordsAppended;
        }

        public ApiFuture<AppendRowsResponse> getFuture() {
            return future;
        }

        public long getExpectedOffset() {
            return expectedOffset;
        }

        public long getRecordsAppended() {
            return recordsAppended;
        }
    }

    /**
     * Following "getters" expose some internal fields required for testing.
     *
     * <p>In addition to keeping these methods package private, ensure that exposed field cannot be
     * changed in a way that alters the class instance's state.
     *
     * <p>Do NOT use these methods outside tests!
     */
    @Internal
    long getAppendRequestSizeBytes() {
        return appendRequestSizeBytes;
    }

    @Internal
    Queue<ApiFuture> getAppendResponseFuturesQueue() {
        return new LinkedList(appendResponseFuturesQueue);
    }

    @Internal
    ProtoRows getProtoRows() {
        return protoRowsBuilder.build();
    }
}
