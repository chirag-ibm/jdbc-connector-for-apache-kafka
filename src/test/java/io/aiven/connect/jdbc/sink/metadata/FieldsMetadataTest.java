/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.connect.jdbc.sink.metadata;

import java.util.*;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.connect.jdbc.sink.JdbcSinkConfig;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FieldsMetadataTest {

    private static final Schema SIMPLE_PRIMITIVE_SCHEMA = Schema.INT64_SCHEMA;
    private static final Schema SIMPLE_STRUCT_SCHEMA = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA).build();
    private static final Schema SIMPLE_MAP_SCHEMA = SchemaBuilder.map(SchemaBuilder.INT64_SCHEMA, Schema.STRING_SCHEMA);

    @Test
    public void valueSchemaMustBePresentForPkModeRecordValue() {
        assertThatThrownBy(() ->
            extract(
                JdbcSinkConfig.PrimaryKeyMode.RECORD_VALUE,
                Collections.emptyList(),
                SIMPLE_PRIMITIVE_SCHEMA,
                null
            )).isInstanceOf(ConnectException.class);
    }

    @Test
    public void valueSchemaMustBeStructIfPresent() {
        assertThatThrownBy(() ->
            extract(
                JdbcSinkConfig.PrimaryKeyMode.KAFKA,
                Collections.emptyList(),
                SIMPLE_PRIMITIVE_SCHEMA,
                SIMPLE_PRIMITIVE_SCHEMA
            )).isInstanceOf(ConnectException.class);
    }

    @Test
    public void missingValueSchemaCanBeOk() {
        final Set<String> name = new HashSet<>();
        name.add("name");
        assertThat(extract(
            JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY,
            Collections.emptyList(),
            SIMPLE_STRUCT_SCHEMA,
            null
        ).allFields.keySet()).isEqualTo(name);

        // this one is a bit weird, only columns being inserted would be kafka coords...
        // but not sure should explicitly disallow!
        assertThat(Lists.newArrayList(extract(
            JdbcSinkConfig.PrimaryKeyMode.KAFKA,
            Collections.emptyList(),
            null,
            null
        ).allFields.keySet())).isEqualTo(Arrays.asList("__connect_topic", "__connect_partition", "__connect_offset"));
    }

    @Test
    public void metadataMayNotBeEmpty() {
        assertThatThrownBy(() ->
            extract(
                JdbcSinkConfig.PrimaryKeyMode.NONE,
                Collections.emptyList(),
                null,
                null
            )).isInstanceOf(ConnectException.class);
    }

    @Test
    public void kafkaPkMode() {
        final FieldsMetadata metadata = extract(
            JdbcSinkConfig.PrimaryKeyMode.KAFKA,
            Collections.emptyList(),
            null,
            SIMPLE_STRUCT_SCHEMA
        );
        assertThat(Lists.newArrayList(metadata.keyFieldNames)).isEqualTo(
            Arrays.asList("__connect_topic", "__connect_partition", "__connect_offset"));

        HashSet<String> nameHashSet = new HashSet<String>() {{
                add("name");
            }};
        assertThat(metadata.nonKeyFieldNames).isEqualTo(nameHashSet);

        final SinkRecordField topicField = metadata.allFields.get("__connect_topic");
        assertThat(topicField.schemaType()).isEqualTo(Schema.Type.STRING);
        assertThat(topicField.isPrimaryKey()).isTrue();
        assertThat(topicField.isOptional()).isFalse();

        final SinkRecordField partitionField = metadata.allFields.get("__connect_partition");
        assertThat(partitionField.schemaType()).isEqualTo(Schema.Type.INT32);
        assertThat(partitionField.isPrimaryKey()).isTrue();
        assertThat(partitionField.isOptional()).isFalse();

        final SinkRecordField offsetField = metadata.allFields.get("__connect_offset");
        assertThat(offsetField.schemaType()).isEqualTo(Schema.Type.INT64);
        assertThat(offsetField.isPrimaryKey()).isTrue();
        assertThat(offsetField.isOptional()).isFalse();
    }

    @Test
    public void kafkaPkModeCustomNames() {
        final List<String> customKeyNames = Arrays.asList("the_topic", "the_partition", "the_offset");
        final FieldsMetadata metadata = extract(
            JdbcSinkConfig.PrimaryKeyMode.KAFKA,
            customKeyNames,
            null,
            SIMPLE_STRUCT_SCHEMA
        );
        assertThat(Lists.newArrayList(metadata.keyFieldNames)).isEqualTo(customKeyNames);
        assertThat(metadata.nonKeyFieldNames).isEqualTo(Collections.singleton("name"));
    }

    @Test
    public void kafkaPkModeBadFieldSpec() {
        assertThatThrownBy(() ->
            extract(
                JdbcSinkConfig.PrimaryKeyMode.KAFKA,
                    Collections.singletonList("lone"),
                null,
                SIMPLE_STRUCT_SCHEMA
            )).isInstanceOf(ConnectException.class);
    }

    /**
     * RECORD_KEY test cases:
     * if keySchema is a struct, pkCols must be a subset of the keySchema fields
     */

    @Test
    public void recordKeyPkModePrimitiveKey() {
        final FieldsMetadata metadata = extract(
            JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY,
                Collections.singletonList("the_pk"),
            SIMPLE_PRIMITIVE_SCHEMA,
            SIMPLE_STRUCT_SCHEMA
        );

        assertThat(metadata.keyFieldNames).isEqualTo(Collections.singleton("the_pk"));

        assertThat(metadata.nonKeyFieldNames).isEqualTo(Collections.singleton("name"));

        assertThat(metadata.allFields.get("the_pk").schemaType()).isEqualTo(SIMPLE_PRIMITIVE_SCHEMA.type());
        assertThat(metadata.allFields.get("the_pk").isPrimaryKey()).isTrue();
        assertThat(metadata.allFields.get("the_pk").isOptional()).isFalse();

        assertThat(metadata.allFields.get("name").schemaType()).isEqualTo(Schema.Type.STRING);
        assertThat(metadata.allFields.get("name").isPrimaryKey()).isFalse();
        assertThat(metadata.allFields.get("name").isOptional()).isFalse();
    }

    @Test
    public void recordKeyPkModeWithPrimitiveKeyButMultiplePkFieldsSpecified() {
        assertThatThrownBy(() ->
            extract(
                JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY,
                Arrays.asList("pk1", "pk2"),
                SIMPLE_PRIMITIVE_SCHEMA,
                SIMPLE_STRUCT_SCHEMA
            )).isInstanceOf(ConnectException.class);
    }

    @Test
    public void recordKeyPkModeButKeySchemaMissing() {
        assertThatThrownBy(() ->
            extract(
                JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY,
                Collections.emptyList(),
                null,
                SIMPLE_STRUCT_SCHEMA
            )).isInstanceOf(ConnectException.class);
    }

    @Test
    public void recordKeyPkModeButKeySchemaAsNonStructCompositeType() {
        assertThatThrownBy(() ->
            extract(
                JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY,
                Collections.emptyList(),
                SIMPLE_MAP_SCHEMA,
                SIMPLE_STRUCT_SCHEMA
            )).isInstanceOf(ConnectException.class);
    }

    @Test
    public void recordKeyPkModeWithStructKeyButMissingField() {
        assertThatThrownBy(() ->
            extract(
                JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY,
                Collections.singletonList("nonexistent"),
                SIMPLE_STRUCT_SCHEMA,
                SIMPLE_STRUCT_SCHEMA
            )).isInstanceOf(ConnectException.class);
    }

    @Test
    public void recordValuePkModeWithMissingPkField() {
        assertThatThrownBy(() ->
            extract(
                JdbcSinkConfig.PrimaryKeyMode.RECORD_VALUE,
                    Collections.singletonList("nonexistent"),
                SIMPLE_PRIMITIVE_SCHEMA,
                SIMPLE_STRUCT_SCHEMA
            )).isInstanceOf(ConnectException.class);
    }

    @Test
    public void recordValuePkModeWithValidPkFields() {
        final FieldsMetadata metadata = extract(
            JdbcSinkConfig.PrimaryKeyMode.RECORD_VALUE,
            Collections.singletonList("name"),
            SIMPLE_PRIMITIVE_SCHEMA,
            SIMPLE_STRUCT_SCHEMA
        );

        assertThat(metadata.keyFieldNames).isEqualTo(new HashSet<String>() {{ add("name"); }});
        assertThat(metadata.nonKeyFieldNames).isEqualTo(Collections.emptySet());

        assertThat(metadata.allFields.get("name").schemaType()).isEqualTo(Schema.Type.STRING);
        assertThat(metadata.allFields.get("name").isPrimaryKey()).isTrue();
        assertThat(metadata.allFields.get("name").isOptional()).isFalse();
    }

    @Test
    public void recordValuePkModeWithPkFieldsAndWhitelistFiltering() {
        final Schema valueSchema =
            SchemaBuilder.struct()
                .field("field1", Schema.INT64_SCHEMA)
                .field("field2", Schema.INT64_SCHEMA)
                .field("field3", Schema.INT64_SCHEMA)
                .field("field4", Schema.INT64_SCHEMA)
                .build();

        final FieldsMetadata metadata = extract(
            JdbcSinkConfig.PrimaryKeyMode.RECORD_VALUE,
            Collections.singletonList("field1"), new HashSet<String>() {{
                add("field2");
                add("field4");
            }},
            null,
            valueSchema
        );

        assertThat(Lists.newArrayList(metadata.keyFieldNames)).isEqualTo(Arrays.asList("field1"));
        assertThat(Lists.newArrayList(metadata.nonKeyFieldNames)).isEqualTo(Arrays.asList("field2", "field4"));
    }

    @Test
    public void recordValuePkModeWithFieldsInOriginalOrdering() {
        final Schema valueSchema =
                SchemaBuilder.struct()
                        .field("field4", Schema.INT64_SCHEMA)
                        .field("field2", Schema.INT64_SCHEMA)
                        .field("field1", Schema.INT64_SCHEMA)
                        .field("field3", Schema.INT64_SCHEMA)
                        .build();

        Set f1 = new HashSet();
        f1.add("field3");
        f1.add("field1");
        f1.add("field2");

        FieldsMetadata metadata = extract(
                JdbcSinkConfig.PrimaryKeyMode.RECORD_VALUE,
                Collections.singletonList("field4"),
                f1,
                null,
                valueSchema
        );

        assertThat(Lists.newArrayList(metadata.allFields.keySet())).isEqualTo(
            Arrays.asList("field4", "field2", "field1", "field3"));

        Set f2 = new HashSet();
        f2.add("field4");
        f2.add("field3");

        metadata = extract(
                JdbcSinkConfig.PrimaryKeyMode.RECORD_VALUE,
                Collections.singletonList("field1"),
                f2,
                null,
                valueSchema
        );

        assertThat(Lists.newArrayList(metadata.allFields.keySet())).isEqualTo(Arrays.asList("field4", "field1", "field3"));

        final Schema keySchema =
                SchemaBuilder.struct()
                        .field("field1", Schema.INT64_SCHEMA)
                        .field("field3", Schema.INT64_SCHEMA)
                        .field("field2", Schema.INT64_SCHEMA)
                        .build();

        Set f3 = new HashSet();
        f3.add("field3");
        f3.add("field1");

        metadata = extract(
                JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY,
                Arrays.asList("field2", "field3", "field1"),
                f3,
                keySchema,
                null
        );

        assertThat(Lists.newArrayList(metadata.allFields.keySet())).isEqualTo(Arrays.asList("field1", "field2", "field3"));
    }

    private static FieldsMetadata extract(final JdbcSinkConfig.PrimaryKeyMode pkMode,
                                          final List<String> pkFields,
                                          final Schema keySchema,
                                          final Schema valueSchema) {
        return extract(pkMode, pkFields, Collections.emptySet(), keySchema, valueSchema);
    }

    private static FieldsMetadata extract(final JdbcSinkConfig.PrimaryKeyMode pkMode,
                                          final List<String> pkFields,
                                          final Set<String> whitelist,
                                          final Schema keySchema,
                                          final Schema valueSchema) {
        return FieldsMetadata.extract("table", pkMode, pkFields, whitelist, keySchema, valueSchema);
    }
}
