/*
 * Copyright (C) 2023 Google Inc.
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

package com.google.cloud.flink.bigquery.services;

import org.apache.flink.annotation.Internal;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamResponse;
import com.google.cloud.bigquery.storage.v1.FlushRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.cloud.flink.bigquery.common.config.CredentialsOptions;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/**
 * Interface defining the behavior to access and operate the needed BigQuery services. This
 * definitions should simplify the faking or mocking of the actual implementations when testing.
 */
@Internal
public interface BigQueryServices extends Serializable {

    /**
     * Retrieves a real, mock or fake {@link QueryDataClient}.
     *
     * @param credentialsOptions The options for the read operation.
     * @return a Query data client for BigQuery.
     */
    QueryDataClient createQueryDataClient(CredentialsOptions credentialsOptions);

    /**
     * Returns a real, mock, or fake {@link StorageReadClient}.
     *
     * @param credentialsOptions The options for the read operation.
     * @return a storage read client object.
     * @throws IOException
     */
    StorageReadClient createStorageReadClient(CredentialsOptions credentialsOptions)
            throws IOException;

    /**
     * Returns a real, mock, or fake {@link StorageWriteClient}.
     *
     * @param credentialsOptions The options for the read operation.
     * @return a storage write client object.
     * @throws IOException
     */
    StorageWriteClient createStorageWriteClient(CredentialsOptions credentialsOptions)
            throws IOException;

    /**
     * Container for reading data from streaming endpoints.
     *
     * <p>An implementation does not need to be thread-safe.
     *
     * @param <T> The type of the response.
     */
    interface BigQueryServerStream<T> extends Iterable<T>, Serializable {
        /**
         * Cancels the stream, releasing any client- and server-side resources. This method may be
         * called multiple times and from any thread.
         */
        void cancel();
    }

    /** An interface representing a client object for invoking BigQuery Storage Read API. */
    interface StorageReadClient extends AutoCloseable {
        /**
         * Create a new BigQuery storage read session against an existing table.
         *
         * @param request the create session request object.
         * @return A BigQuery storage read session.
         */
        ReadSession createReadSession(CreateReadSessionRequest request);

        /**
         * Read rows in the context of a specific read stream.
         *
         * @param request The request for the storage API
         * @return a server stream response with the read rows.
         */
        BigQueryServerStream<ReadRowsResponse> readRows(ReadRowsRequest request);

        /**
         * Close the client object.
         *
         * <p>The override is required since {@link AutoCloseable} allows the close method to raise
         * an exception.
         */
        @Override
        void close();
    }

    /** An interface representing a client object for invoking BigQuery Storage Write API. */
    interface StorageWriteClient extends AutoCloseable {
        /**
         * Create a StreamWriter for writing to a BigQuery table.
         *
         * @param streamName the write stream to be used by this writer.
         * @param protoSchema the schema of the serialized protocol buffer data rows.
         * @param enableConnectionPool enable BigQuery client multiplexing for this writer.
         * @param traceId job identifier.
         * @return A StreamWriter for a BigQuery storage write stream.
         * @throws IOException
         */
        StreamWriter createStreamWriter(
                String streamName,
                ProtoSchema protoSchema,
                boolean enableConnectionPool,
                String traceId)
                throws IOException;

        /**
         * Create a write stream for a BigQuery table.
         *
         * @param tablePath the table to which the stream belongs.
         * @param streamType the type of the stream.
         * @return A WriteStream.
         */
        WriteStream createWriteStream(String tablePath, WriteStream.Type streamType);

        /**
         * Flush data in buffered stream to BigQuery table.
         *
         * @param streamName the write stream to be flushed.
         * @param offset the offset to which write stream must be flushed.
         * @return A FlushRowsResponse.
         */
        FlushRowsResponse flushRows(String streamName, long offset);

        /**
         * Finalize a BigQuery write stream so that no new data can be appended to the stream.
         *
         * @param streamName the write stream to be finalized.
         * @return A FinalizeWriteStreamResponse.
         */
        FinalizeWriteStreamResponse finalizeWriteStream(String streamName);

        /**
         * Close the client object.
         *
         * <p>The override is required since {@link AutoCloseable} allows the close method to raise
         * an exception.
         */
        @Override
        void close();
    }

    /**
     * An interface representing the client interactions needed to retrieve data from BigQuery using
     * SQL queries.
     */
    interface QueryDataClient extends Serializable {
        /**
         * Returns a list with the table's existing partitions.
         *
         * @param project The GCP project.
         * @param dataset The BigQuery dataset.
         * @param table The BigQuery table.
         * @return A list of the partition identifiers.
         */
        List<String> retrieveTablePartitions(String project, String dataset, String table);

        /**
         * Returns, in case of having one, the partition column information for the table.
         *
         * @param project The GCP project.
         * @param dataset The BigQuery dataset.
         * @param table The BigQuery table.
         * @return The information of the table's partition.
         */
        Optional<TablePartitionInfo> retrievePartitionColumnInfo(
                String project, String dataset, String table);

        /**
         * Returns the {@link TableSchema} of the specified BigQuery table.
         *
         * @param project The GCP project.
         * @param dataset The BigQuery dataset.
         * @param table The BigQuery table.
         * @return The BigQuery table {@link TableSchema}.
         */
        TableSchema getTableSchema(String project, String dataset, String table);

        /**
         * Get BigQuery dataset.
         *
         * @param project The GCP project.
         * @param dataset The BigQuery dataset.
         * @return The BigQuery {@link Dataset}.
         */
        Dataset getDataset(String project, String dataset);

        /**
         * Create BigQuery dataset.
         *
         * @param project The GCP project.
         * @param dataset The BigQuery dataset.
         * @param region GCP region where dataset must be created.
         */
        void createDataset(String project, String dataset, String region);

        /**
         * Function to identify if a BigQuery table exists.
         *
         * @param project The project ID of the BigQuery dataset
         * @param dataset The BigQuery dataset.
         * @param table The BigQuery table.
         * @return Boolean {@code TRUE} if the table exists or {@code FALSE} if it does not.
         */
        Boolean tableExists(String project, String dataset, String table);

        /**
         * Function create a BigQuery table.
         *
         * @param project The GCP project.
         * @param dataset The BigQuery dataset.
         * @param table The BigQuery table.
         * @param tableDefinition Description of BigQuery table.
         */
        void createTable(
                String project, String dataset, String table, TableDefinition tableDefinition);
    }
}
