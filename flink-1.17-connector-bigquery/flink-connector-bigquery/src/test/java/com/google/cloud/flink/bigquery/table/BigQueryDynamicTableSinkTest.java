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

package com.google.cloud.flink.bigquery.table;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.test.junit5.MiniClusterExtension;

import com.google.cloud.flink.bigquery.fakes.StorageClientFaker;
import com.google.cloud.flink.bigquery.sink.BigQuerySinkConfig;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/** Class to test {@link BigQueryDynamicTableSink}. */
public class BigQueryDynamicTableSinkTest {

    static LogicalType logicalTypeSchema =
            DataTypes.ROW(DataTypes.FIELD("number", DataTypes.BIGINT().notNull()))
                    .notNull()
                    .getLogicalType();

    private static final int PARALLELISM = 5;

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(PARALLELISM)
                            .build());

    @Test
    public void testConstructor() {
        BigQueryDynamicTableSink bigQueryDynamicTableSink =
                new BigQueryDynamicTableSink(
                        StorageClientFaker.createConnectOptionsForWrite(null),
                        DeliveryGuarantee.AT_LEAST_ONCE,
                        logicalTypeSchema,
                        PARALLELISM);
        BigQuerySinkConfig obtainedSinkConfig = bigQueryDynamicTableSink.getSinkConfig();
        assertEquals(logicalTypeSchema, bigQueryDynamicTableSink.getLogicalType());
        assertEquals(DeliveryGuarantee.AT_LEAST_ONCE, obtainedSinkConfig.getDeliveryGuarantee());
        Schema convertedAvroSchema =
                new Schema.Parser()
                        .parse(
                                "{\"type\":\"record\",\"name\":\"record\","
                                        + "\"namespace\":\"org.apache.flink.avro.generated\",\"fields\":"
                                        + "[{\"name\":\"number\",\"type\":\"long\"}]}");
        assertEquals(convertedAvroSchema, obtainedSinkConfig.getSchemaProvider().getAvroSchema());
        assertEquals(PARALLELISM, bigQueryDynamicTableSink.getSinkParallelism());
    }

    @Test
    public void testCopy() {
        BigQueryDynamicTableSink bigQueryDynamicTableSink =
                new BigQueryDynamicTableSink(
                        StorageClientFaker.createConnectOptionsForWrite(null),
                        DeliveryGuarantee.AT_LEAST_ONCE,
                        logicalTypeSchema);
        assertTrue(bigQueryDynamicTableSink.equals(bigQueryDynamicTableSink.copy()));
    }

    @Test
    public void testCopyWithParallelism() {
        BigQueryDynamicTableSink bigQueryDynamicTableSink =
                new BigQueryDynamicTableSink(
                        StorageClientFaker.createConnectOptionsForWrite(null),
                        DeliveryGuarantee.AT_LEAST_ONCE,
                        logicalTypeSchema,
                        PARALLELISM);
        assertTrue(bigQueryDynamicTableSink.equals(bigQueryDynamicTableSink.copy()));
    }

    @Test
    public void testSummaryString() {
        BigQueryDynamicTableSink bigQueryDynamicTableSink =
                new BigQueryDynamicTableSink(
                        StorageClientFaker.createConnectOptionsForWrite(null),
                        DeliveryGuarantee.AT_LEAST_ONCE,
                        logicalTypeSchema);
        assertEquals("BigQuery", bigQueryDynamicTableSink.asSummaryString());
    }

    @Test
    public void testChangelogMode() {
        BigQueryDynamicTableSink bigQueryDynamicTableSink =
                new BigQueryDynamicTableSink(
                        StorageClientFaker.createConnectOptionsForWrite(null),
                        DeliveryGuarantee.AT_LEAST_ONCE,
                        logicalTypeSchema);
        assertEquals(
                ChangelogMode.insertOnly(),
                bigQueryDynamicTableSink.getChangelogMode(Mockito.mock(ChangelogMode.class)));
    }

    @Test
    public void testSinkRuntimeProvider() {
        BigQueryDynamicTableSink bigQueryDynamicTableSink =
                new BigQueryDynamicTableSink(
                        StorageClientFaker.createConnectOptionsForWrite(null),
                        DeliveryGuarantee.AT_LEAST_ONCE,
                        logicalTypeSchema);
        assertInstanceOf(
                SinkV2Provider.class,
                bigQueryDynamicTableSink.getSinkRuntimeProvider(
                        Mockito.mock(DynamicTableSink.Context.class)));
    }
}
