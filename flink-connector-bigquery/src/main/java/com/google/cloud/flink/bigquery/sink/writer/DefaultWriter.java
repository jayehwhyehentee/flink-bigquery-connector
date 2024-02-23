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

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.cloud.flink.bigquery.sink.serializer.BigQueryProtoSerializer;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Writer implementation for {@link DefaultSink}.
 *
 * <p>This writer appends records to the BigQuery table's default write stream. This means that
 * records are written directly to the table with no additional commit required, and available for
 * querying immediately.
 *
 * <p>In case of stream replay upon failure recovery, records will be written again, regardless of
 * appends prior to the application's failure.
 *
 * <p>Records are grouped to maximally utilize the BigQuery append request's payload.
 *
 * @param <IN> Type of records to be written to BigQuery.
 */
public class DefaultWriter<IN> extends BaseWriter<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultWriter.class);

    public DefaultWriter(
            BigQueryConnectOptions connectOptions,
            ProtoSchema protoSchema,
            BigQueryProtoSerializer serializer,
            String tablePath) {
        super(connectOptions, protoSchema, serializer);
        streamName = String.format("%s/streams/_default", tablePath);
        streamWriter = createStreamWriter();
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        try {
            ByteString protoRow = getProtoRow(element);
            if (!fitsInAppendRequest(protoRow)) {
                validateAppendResponses(false);
                append();
            }
            addToAppendRequest(protoRow);
        } catch (BigQuerySerializationException e) {
            LOG.error(String.format("Unable to serialize record {%s}. Dropping it!", element), e);
        }
    }

    @Override
    void sendAppendRequest() {
        ApiFuture responseFuture = streamWriter.append(protoRowsBuilder.build());
        appendResponseFuturesQueue.add(responseFuture);
    }

    @Override
    void validateAppendResponses(boolean waitForResponse) {
        ApiFuture<AppendRowsResponse> appendResponseFuture;
        while ((appendResponseFuture = appendResponseFuturesQueue.peek()) != null) {
            if (waitForResponse || appendResponseFuture.isDone()) {
                appendResponseFuturesQueue.poll();
                boolean succeeded = false;
                try {
                  AppendRowsResponse response = appendResponseFuture.get();
                  // If we got here, the response was successful.
                  succeeded = true;
                }
                catch (ExecutionException e) {}
                catch (InterruptedException e) {}
                finally {}
            }
            else {
                break;
            }
        }
    }
    

}
