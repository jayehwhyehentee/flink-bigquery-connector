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

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.BigQueryServicesFactory;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQueryConnectorException;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.cloud.flink.bigquery.sink.serializer.BigQueryProtoSerializer;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Base class for developing a BigQuery writer.
 *
 * <p>This class abstracts implementation details common for BigQuery writers which use the {@link
 * StreamWriter}.
 *
 * @param <IN> Type of records to be written to BigQuery.
 */
abstract class BaseWriter<IN> implements SinkWriter<IN> {

    // Multiply 0.95 to keep a buffer from exceeding payload limits.
    private static final long MAX_APPEND_REQUEST_BYTES =
            (long) (StreamWriter.getApiMaxRequestBytes() * 0.95);

    // Number of bytes to be sent in the next append request.
    private long appendRequestSizeBytes;
    private final BigQueryConnectOptions connectOptions;
    private final ProtoSchema protoSchema;
    private final BigQueryProtoSerializer serializer;

    Queue<ApiFuture> appendResponseFuturesQueue;
    ProtoRows.Builder protoRowsBuilder;
    StreamWriter streamWriter;
    String streamName;

    BaseWriter(
            BigQueryConnectOptions connectOptions,
            ProtoSchema protoSchema,
            BigQueryProtoSerializer serializer) {
        this.connectOptions = connectOptions;
        this.protoSchema = protoSchema;
        this.serializer = serializer;
        appendRequestSizeBytes = 0L;
        appendResponseFuturesQueue = new LinkedList<>();
        protoRowsBuilder = ProtoRows.newBuilder();
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (appendRequestSizeBytes > 0) {
            append();
        }
        validateAppendResponses(true);
        // TODO: flush needs to check all remaining async append results
    }

    @Override
    public void close() {
        if (streamWriter != null) {
            streamWriter.close();
        }
    }

    abstract void sendAppendRequest();

    /**
     * Throws an exception if there is an error in any of the responses received thus far. Since
     * responses arrive in order, we proceed to check the next response only after the previous
     * response has arrived.
     */
    abstract void validateAppendResponses(boolean waitForResponse);

    void addToAppendRequest(ByteString protoRow) {
        protoRowsBuilder.addSerializedRows(protoRow);
        appendRequestSizeBytes += getProtoRowBytes(protoRow);
    }

    void append() {
        sendAppendRequest();
        protoRowsBuilder.clear();
        appendRequestSizeBytes = 0L;
    }

    StreamWriter createStreamWriter() throws BigQueryConnectorException {
        try (BigQueryServices.StorageWriteClient client =
                BigQueryServicesFactory.instance(connectOptions).storageWrite()) {
            return client.createStreamWriter(streamName, protoSchema);
        } catch (IOException e) {
            throw new BigQueryConnectorException(
                    String.format("Unable to create StreamWriter for stream ", streamName), e);
        }
    }

    boolean fitsInAppendRequest(ByteString protoRow) {
        return appendRequestSizeBytes + getProtoRowBytes(protoRow) <= MAX_APPEND_REQUEST_BYTES;
    }

    ByteString getProtoRow(IN element) throws BigQuerySerializationException {
        ByteString protoRow = serializer.serialize(element);
        if (getProtoRowBytes(protoRow) > MAX_APPEND_REQUEST_BYTES) {
            throw new BigQuerySerializationException(
                    String.format(
                            "A single row of size %d bytes exceeded the allowed maximum of %d bytes for an append request",
                            getProtoRowBytes(protoRow), MAX_APPEND_REQUEST_BYTES));
        }
        return protoRow;
    }

    private int getProtoRowBytes(ByteString protoRow) {
        // Protobuf overhead is at least 2 bytes per row.
        return protoRow.size() + 2;
    }
}
