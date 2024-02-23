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

package com.google.cloud.flink.bigquery.sink;

import org.apache.flink.api.connector.sink2.Sink;

import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.sink.serializer.BigQueryProtoSerializer;

/** Base class for developing a BigQuery sink. */
abstract class BaseSink implements Sink {

    final BigQueryConnectOptions connectOptions;
    final ProtoSchema protoSchema;
    final BigQueryProtoSerializer serializer;
    final String tablePath;

    BaseSink(
            BigQueryConnectOptions connectOptions,
            ProtoSchema protoSchema,
            BigQueryProtoSerializer serializer) {
        this.connectOptions = connectOptions;
        this.protoSchema = protoSchema;
        this.serializer = serializer;
        this.tablePath =
                String.format(
                        "projects/%s/datasets/%s/tables/%s",
                        connectOptions.getProjectId(),
                        connectOptions.getDataset(),
                        connectOptions.getTable());
    }
}
