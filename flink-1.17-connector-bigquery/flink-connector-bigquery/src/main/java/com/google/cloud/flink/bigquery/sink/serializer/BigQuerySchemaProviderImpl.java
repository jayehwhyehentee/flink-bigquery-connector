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

package com.google.cloud.flink.bigquery.sink.serializer;

import com.google.api.client.util.Preconditions;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.common.exceptions.BigQueryConnectorException;
import com.google.cloud.flink.bigquery.common.utils.SchemaTransform;
import com.google.cloud.flink.bigquery.services.BigQueryUtils;
import com.google.cloud.flink.bigquery.sink.client.BigQueryClientWithErrorHandling;
import com.google.cloud.flink.bigquery.sink.serializer.AvroToProtoSerializer.AvroSchemaHandler;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.commons.lang3.tuple.ImmutablePair;

import javax.annotation.Nullable;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Default implementation of {@link BigQuerySchemaProvider} deriving Avro {@link Schema} from {@link
 * TableSchema}, which in turn is sourced from {@link BigQueryConnectOptions}.
 */
public class BigQuerySchemaProviderImpl implements BigQuerySchemaProvider {

    private final Schema avroSchema;
    private final DescriptorProto descriptorProto;

    private static final Map<Schema.Type, FieldDescriptorProto.Type> AVRO_TYPES_TO_PROTO;
    private static final Map<String, FieldDescriptorProto.Type> LOGICAL_AVRO_TYPES_TO_PROTO;

    public BigQuerySchemaProviderImpl(BigQueryConnectOptions connectOptions) {
        if (BigQueryClientWithErrorHandling.tableExists(connectOptions)) {
            TableSchema tableSchema =
                    BigQueryClientWithErrorHandling.getTableSchema(connectOptions);
            avroSchema = getAvroSchema(tableSchema);
            descriptorProto = getDescriptorSchemaFromAvroSchema(avroSchema);
            return;
        }
        avroSchema = null;
        descriptorProto = null;
    }

    public BigQuerySchemaProviderImpl(TableSchema tableSchema) {
        this(getAvroSchema(tableSchema));
    }

    public BigQuerySchemaProviderImpl(Schema schema) {
        avroSchema = schema;
        descriptorProto = getDescriptorSchemaFromAvroSchema(avroSchema);
    }

    @Override
    public DescriptorProto getDescriptorProto() {
        return descriptorProto;
    }

    @Override
    public Descriptor getDescriptor() {
        try {
            return getDescriptorFromDescriptorProto(descriptorProto);
        } catch (DescriptorValidationException | IllegalArgumentException e) {
            throw new BigQueryConnectorException(
                    "Could not obtain Descriptor for BigQuery table", e);
        }
    }

    @Override
    public Schema getAvroSchema() {
        return avroSchema;
    }

    @Override
    public boolean schemaUnknown() {
        return avroSchema == null;
    }

    @Override
    public int hashCode() {
        Schema thisAvroSchema = getAvroSchema();
        if (thisAvroSchema == null) {
            return Integer.MIN_VALUE;
        }
        return thisAvroSchema.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        return Objects.equals(getAvroSchema(), ((BigQuerySchemaProviderImpl) obj).getAvroSchema());
    }

    // ----------- Initialize Maps between Avro Schema to Descriptor Proto schema -------------
    static {
        /*
         * Map Avro Schema Type to FieldDescriptorProto Type which converts AvroSchema
         * Primitive Type to Dynamic Message.
         * AVRO_TYPES_TO_PROTO: containing mapping from Primitive Avro Schema Type to FieldDescriptorProto.
         */
        AVRO_TYPES_TO_PROTO = new EnumMap<>(Schema.Type.class);
        AVRO_TYPES_TO_PROTO.put(Schema.Type.FIXED, FieldDescriptorProto.Type.TYPE_BYTES);
        AVRO_TYPES_TO_PROTO.put(Schema.Type.INT, FieldDescriptorProto.Type.TYPE_INT64);
        AVRO_TYPES_TO_PROTO.put(Schema.Type.LONG, FieldDescriptorProto.Type.TYPE_INT64);
        AVRO_TYPES_TO_PROTO.put(Schema.Type.FLOAT, FieldDescriptorProto.Type.TYPE_DOUBLE);
        AVRO_TYPES_TO_PROTO.put(Schema.Type.DOUBLE, FieldDescriptorProto.Type.TYPE_DOUBLE);
        AVRO_TYPES_TO_PROTO.put(Schema.Type.STRING, FieldDescriptorProto.Type.TYPE_STRING);
        AVRO_TYPES_TO_PROTO.put(Schema.Type.BOOLEAN, FieldDescriptorProto.Type.TYPE_BOOL);
        AVRO_TYPES_TO_PROTO.put(Schema.Type.ENUM, FieldDescriptorProto.Type.TYPE_STRING);
        AVRO_TYPES_TO_PROTO.put(Schema.Type.BYTES, FieldDescriptorProto.Type.TYPE_BYTES);

        /*
         * Map Logical Avro Schema Type to FieldDescriptorProto Type, which converts
         * AvroSchema Primitive Type to Dynamic Message.
         * LOGICAL_AVRO_TYPES_TO_PROTO: Map containing mapping from Primitive Avro Schema Type to FieldDescriptorProto.
         */
        LOGICAL_AVRO_TYPES_TO_PROTO = new HashMap<>();
        LOGICAL_AVRO_TYPES_TO_PROTO.put(
                LogicalTypes.date().getName(), FieldDescriptorProto.Type.TYPE_INT32);
        LOGICAL_AVRO_TYPES_TO_PROTO.put(
                LogicalTypes.decimal(1).getName(), FieldDescriptorProto.Type.TYPE_BYTES);
        LOGICAL_AVRO_TYPES_TO_PROTO.put(
                LogicalTypes.timestampMicros().getName(), FieldDescriptorProto.Type.TYPE_INT64);
        LOGICAL_AVRO_TYPES_TO_PROTO.put(
                LogicalTypes.timestampMillis().getName(), FieldDescriptorProto.Type.TYPE_INT64);
        LOGICAL_AVRO_TYPES_TO_PROTO.put(
                LogicalTypes.uuid().getName(), FieldDescriptorProto.Type.TYPE_STRING);
        // These are newly added.
        LOGICAL_AVRO_TYPES_TO_PROTO.put(
                LogicalTypes.timeMillis().getName(), FieldDescriptorProto.Type.TYPE_STRING);
        LOGICAL_AVRO_TYPES_TO_PROTO.put(
                LogicalTypes.timeMicros().getName(), FieldDescriptorProto.Type.TYPE_STRING);
        LOGICAL_AVRO_TYPES_TO_PROTO.put(
                LogicalTypes.localTimestampMillis().getName(),
                FieldDescriptorProto.Type.TYPE_STRING);
        LOGICAL_AVRO_TYPES_TO_PROTO.put(
                LogicalTypes.localTimestampMicros().getName(),
                FieldDescriptorProto.Type.TYPE_STRING);
        LOGICAL_AVRO_TYPES_TO_PROTO.put("geography_wkt", FieldDescriptorProto.Type.TYPE_STRING);
        LOGICAL_AVRO_TYPES_TO_PROTO.put("Json", FieldDescriptorProto.Type.TYPE_STRING);
    }

    // --------------- Obtain AvroSchema from TableSchema -----------------
    /**
     * Function to convert TableSchema to Avro Schema.
     *
     * @param tableSchema A {@link TableSchema} object to cast to {@link Schema}.
     * @return Converted Avro Schema
     */
    private static Schema getAvroSchema(TableSchema tableSchema) {
        return SchemaTransform.toGenericAvroSchema("root", tableSchema.getFields());
    }

    // --------------- Obtain Descriptor Proto from Avro Schema  ---------------
    /**
     * Obtains a Descriptor Proto by obtaining Descriptor Proto field by field.
     *
     * <p>Iterates over Avro Schema to obtain the FieldDescriptorProto for it.
     *
     * @param schema Avro Schema, for which descriptor is needed.
     * @return DescriptorProto describing the Schema.
     */
    private static DescriptorProto getDescriptorSchemaFromAvroSchema(Schema schema) {
        Preconditions.checkState(!schema.getFields().isEmpty());
        DescriptorProto.Builder descriptorBuilder = DescriptorProto.newBuilder();
        // Obtain a unique name for the descriptor ('-' characters cannot be used).
        descriptorBuilder.setName(BigQueryUtils.bqSanitizedRandomUUIDForDescriptor());
        int i = 1;
        // Iterate over each table field and add them to the schema.
        for (Schema.Field field : schema.getFields()) {
            fieldDescriptorFromSchemaField(field, i++, descriptorBuilder);
        }
        return descriptorBuilder.build();
    }

    /**
     * Function to obtain the FieldDescriptorProto from a AvroSchemaField and then append it to
     * DescriptorProto builder.
     *
     * @param field {@link Schema.Field} object to obtain the {@link FieldDescriptorProto} from.
     * @param fieldNumber index at which the obtained {@link FieldDescriptorProto} is appended in
     *     the Descriptor.
     * @param descriptorProtoBuilder {@link DescriptorProto.Builder} object to add the obtained
     *     {@link FieldDescriptorProto} to.
     */
    private static void fieldDescriptorFromSchemaField(
            Schema.Field field, int fieldNumber, DescriptorProto.Builder descriptorProtoBuilder) {

        @Nullable Schema schema = field.schema();
        Preconditions.checkNotNull(schema, "Unexpected null schema!");

        FieldDescriptorProto.Builder fieldDescriptorBuilder =
                FieldDescriptorProto.newBuilder()
                        .setName(field.name().toLowerCase())
                        .setNumber(fieldNumber);

        boolean isNullable = false;
        switch (schema.getType()) {
            case RECORD:
                Preconditions.checkState(!schema.getFields().isEmpty());
                /*
                Recursion to obtain the descriptor for each field inside the record.
                Add the converted descriptor as a nested type.
                Set the current fieldDescriptor type as "MESSAGE" with type name as descriptor name.
                 */
                DescriptorProto nested = getDescriptorSchemaFromAvroSchema(schema);
                descriptorProtoBuilder.addNestedType(nested);
                fieldDescriptorBuilder
                        .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName(nested.getName());
                break;
            case ARRAY:
                fieldDescriptorBuilder =
                        getDescriptorProtoForArraySchema(
                                schema, field, fieldNumber, descriptorProtoBuilder);
                break;
            case MAP:
                throw new UnsupportedOperationException("MAP type not supported yet.");
            case UNION:
                /* Union schemas can mainly be of the following types:
                1. Only null value (["null"])
                2. ONE non-null value ["datatype1"]
                3. ONE non-null value with a null (["datatype1", "null"])
                4. Multiple non-null and a null value (["datatype1", "datatype2", ...,  "null"])
                5. Only Multiple non-null values ["datatype1", "datatype2", ... ]

                Types 1, 4 and 5 - are not supported by Bigquery; An error is thrown here.
                Type 2 - is the same as a REQUIRED or a standard primitive type.
                Type 3 - indicates the use of null values along with a datatype.
                This is mapped to OPTIONAL field in BigQuery.
                */
                ImmutablePair<Schema, Boolean> handleUnionSchemaResult =
                        AvroSchemaHandler.handleUnionSchema(schema);
                schema = handleUnionSchemaResult.getLeft();
                isNullable = handleUnionSchemaResult.getRight();
                fieldDescriptorBuilder =
                        getDescriptorProtoForUnionSchema(
                                schema, isNullable, field, fieldNumber, descriptorProtoBuilder);
                break;
            default:
                getDescriptorProtoForPrimitiveAndLogicalSchema(
                        schema, fieldDescriptorBuilder, field);
        }
        // Set the Labels for different Modes - REPEATED, REQUIRED, NULLABLE.
        if (fieldDescriptorBuilder.getLabel() != FieldDescriptorProto.Label.LABEL_REPEATED) {
            if (isNullable) {
                fieldDescriptorBuilder.setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
            } else {
                // The Default Value is specified only in the case of scalar non-repeated fields.
                // If it was a scalar type, the default value would already have been set.
                fieldDescriptorBuilder.setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED);
            }
        }
        descriptorProtoBuilder.addField(fieldDescriptorBuilder.build());
    }
    // --------------- Helper Functions to convert AvroSchema to DescriptorProto ---------------

    /**
     * Helper function to convert the UNION type schema field to a FieldDescriptorProto.
     *
     * @param elementType {@link Schema} object defining the data type within the UNION.
     * @param isNullable boolean value indicating if the descriptor field is NULLABLE.
     * @param field {@link Schema.Field} object of the UNION type field.
     * @param fieldNumber the field number to add the derived FieldDescriptorProto to.
     * @param descriptorProtoBuilder The updated {@link DescriptorProto.Builder}
     * @return {@link FieldDescriptorProto.Builder} obtained for the UNION schema field.
     * @throws IllegalArgumentException If the elementType is not ["null","datatype"] or
     *     ["datatype"].
     * @throws UnsupportedOperationException In case schema of a type ["null", "MAP"] or ["null",
     *     "ARRAY"] is obtained.
     */
    private static FieldDescriptorProto.Builder getDescriptorProtoForUnionSchema(
            Schema elementType,
            boolean isNullable,
            Schema.Field field,
            int fieldNumber,
            DescriptorProto.Builder descriptorProtoBuilder)
            throws IllegalArgumentException, UnsupportedOperationException {
        // This method is called only if we have the types elementType ->
        // ["null","datatype"]/["datatype"].
        if (elementType == null) {
            throw new IllegalArgumentException("Unexpected null element type!");
        }
        /* UNION of type MAP and ARRAY is not supported.
        ARRAY is mapped to REPEATED type in Bigquery, which cannot be OPTIONAL.
        MAP datatype is mapped to "REPEATED field of type MESSAGE,"
        which cannot be OPTIONAL.
        If we have the datatype is ["null", "MAP"] or ["null", "ARRAY"],
        UnsupportedOperationException is thrown. */
        if (isNullable
                && (elementType.getType() == Schema.Type.MAP
                        || elementType.getType() == Schema.Type.ARRAY)) {
            throw new UnsupportedOperationException(
                    "NULLABLE MAP/ARRAYS in UNION types are not supported");
        }
        /* Obtain the descriptor for the non-null datatype in the UNION schema.
        Set the field as NULLABLE in case UNION of a type ["null", datatype]
        as REQUIRED in case UNION of a type [datatype]
        Add any nested types obtained to the descriptorProto Builder. */
        DescriptorProto.Builder unionFieldBuilder = DescriptorProto.newBuilder();
        fieldDescriptorFromSchemaField(
                new Schema.Field(field.name(), elementType, field.doc(), field.defaultVal()),
                fieldNumber,
                unionFieldBuilder);
        descriptorProtoBuilder.addAllNestedType(unionFieldBuilder.getNestedTypeList());
        return unionFieldBuilder.getFieldBuilder(0);
    }

    /**
     * Helper function to update the FieldDescriptorBuilder for Primitive and Logical Datatypes.
     *
     * <p><i>LOGICAL</i>: Use elementType.getProp() to obtain the string for the property name and
     * search for its corresponding mapping in the LOGICAL_AVRO_TYPES_TO_PROTO map.
     *
     * <p><i>PRIMITIVE</i>: If there is no match for the logical type (or there is no logical type
     * present), the element data type is attempted to be mapped to a PRIMITIVE type map.
     *
     * @param elementType {@link Schema} object for Primitive or Logical data-type.
     * @param fieldDescriptorBuilder {@link FieldDescriptorProto.Builder} object to update.
     * @param field {@link Schema.Field} object of the primitive/logical data-type field.
     * @throws UnsupportedOperationException If NO match is found for any of the primitive or
     *     logical types.
     */
    private static void getDescriptorProtoForPrimitiveAndLogicalSchema(
            Schema elementType,
            FieldDescriptorProto.Builder fieldDescriptorBuilder,
            Schema.Field field)
            throws UnsupportedOperationException {
        @Nullable
        FieldDescriptorProto.Type type =
                Optional.ofNullable(elementType.getProp(LogicalType.LOGICAL_TYPE_PROP))
                        .map(LOGICAL_AVRO_TYPES_TO_PROTO::get)
                        .orElse(AVRO_TYPES_TO_PROTO.get(elementType.getType()));
        if (type == null) {
            throw new UnsupportedOperationException(
                    "Converting AVRO type "
                            + elementType.getType()
                            + " to Storage API Proto type is unsupported");
        }
        /* The corresponding type obtained(if obtained) is set in the fieldDescriptor.
         * Any default value present in the schema is also set in descriptor.*/
        fieldDescriptorBuilder.setType(type);
        if (field.hasDefaultValue()) {
            fieldDescriptorBuilder.setDefaultValue(field.defaultVal().toString());
        }
    }

    /**
     * Function to derive the Field Descriptor for a Schema Field with ARRAY data-type.
     *
     * <p>A bigquery field is created by the provided data-type (mapped according to the rules
     * described), and the field MODE is marked as <b>REPEATED</b>.
     *
     * @param avroSchema {@link Schema} object for ARRAY Schema field.
     * @param field {@link Schema.Field} object of the ARRAY data-type field.
     * @param fieldNumber the field number to add the derived FieldDescriptorProto to.
     * @param descriptorProtoBuilder The {@link DescriptorProto.Builder} to be updated.
     * @return {@link FieldDescriptorProto.Builder} obtained for the ARRAY schema field.
     * @throws UnsupportedOperationException If ARRAY of type MAP is passed.
     * @throws IllegalArgumentException If ARRAY of type UNION or NULL schema is passed.
     */
    private static FieldDescriptorProto.Builder getDescriptorProtoForArraySchema(
            Schema avroSchema,
            Schema.Field field,
            Integer fieldNumber,
            DescriptorProto.Builder descriptorProtoBuilder)
            throws UnsupportedOperationException {
        Schema elementType = avroSchema.getElementType();
        if (elementType == null) {
            throw new IllegalArgumentException("Unexpected null element type!");
        }
        Preconditions.checkState(
                elementType.getType() != Schema.Type.ARRAY,
                "Nested arrays not supported by BigQuery.");
        if (elementType.getType() == Schema.Type.MAP) {
            // Note this case would be covered in the check which is performed later,
            // but just in case the support for this is provided in the future,
            // it is explicitly mentioned.
            throw new UnsupportedOperationException("Array of Type MAP not supported yet.");
        }
        // Create the descriptor for datatype present in ARRAY type.
        DescriptorProto.Builder arrayFieldBuilder = DescriptorProto.newBuilder();
        fieldDescriptorFromSchemaField(
                new Schema.Field(field.name(), elementType, field.doc(), field.defaultVal()),
                fieldNumber,
                arrayFieldBuilder);

        FieldDescriptorProto.Builder arrayFieldElementBuilder =
                arrayFieldBuilder.getFieldBuilder(0);
        // Check if the inner field is optional without any default value.
        if (arrayFieldElementBuilder.getLabel() != FieldDescriptorProto.Label.LABEL_REQUIRED) {
            throw new IllegalArgumentException("Array cannot have a NULLABLE element");
        }

        // Add any nested types.
        descriptorProtoBuilder.addAllNestedType(arrayFieldBuilder.getNestedTypeList());
        // Default value derived from inner layers should not be the default value of array
        // field.
        return arrayFieldElementBuilder
                .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)
                .clearDefaultValue();
    }

    // --------------- Obtain Descriptor from DescriptorProto  ---------------
    /**
     * Function to convert a DescriptorProto to a Descriptor. This is necessary as a Descriptor is
     * needed for {@link com.google.protobuf.DynamicMessage} (used to write to Storage API).
     *
     * @param descriptorProto input which needs to be converted to a {@link Descriptor}.
     * @return {@link Descriptor} obtained form the input {@link DescriptorProto}
     * @throws DescriptorValidationException in case the conversion is not possible.
     */
    private static Descriptor getDescriptorFromDescriptorProto(DescriptorProto descriptorProto)
            throws DescriptorValidationException {
        FileDescriptorProto fileDescriptorProto =
                FileDescriptorProto.newBuilder().addMessageType(descriptorProto).build();
        FileDescriptor fileDescriptor =
                FileDescriptor.buildFrom(fileDescriptorProto, new FileDescriptor[0]);
        List<Descriptor> descriptorTypeList = fileDescriptor.getMessageTypes();
        if (descriptorTypeList.size() == 1) {
            return descriptorTypeList.get(0);
        } else {
            throw new IllegalArgumentException(
                    String.format("Expected one element but was %s", descriptorTypeList));
        }
    }
}
