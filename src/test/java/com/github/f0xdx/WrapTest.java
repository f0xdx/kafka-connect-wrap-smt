/*
 * Copyright 2020 the kafka-connect-wrap-smt authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.f0xdx;

import static com.github.f0xdx.Schemas.optionalSchemaOrElse;
import static com.github.f0xdx.Schemas.toBuilder;
import static com.github.f0xdx.Wrap.*;
import static org.apache.kafka.common.record.TimestampType.CREATE_TIME;
import static org.apache.kafka.connect.data.Schema.*;
import static org.apache.kafka.connect.data.Schema.Type.INT32;
import static org.apache.kafka.connect.data.Schema.Type.STRUCT;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;
import java.util.Map;
import lombok.val;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class WrapTest {

  Wrap<SinkRecord> transform;

  @BeforeEach
  void setUp() {
    transform = new Wrap<>();
    transform.configure(Collections.emptyMap());
  }

  @DisplayName("apply (source record)")
  @Test
  void applySourceRecord() {
    val transform = new Wrap<SourceRecord>();
    transform.configure(Collections.emptyMap());

    val sourceRecord = new SourceRecord(null, null, "topic", STRING_SCHEMA, "source");

    assertThrows(DataException.class, () -> transform.apply(sourceRecord));
  }

  @DisplayName("obtain last key schema (no last schema)")
  @Test
  void lastKeySchemaWithoutLastSchema() {
    assertEquals(OPTIONAL_STRING_SCHEMA, transform.lastKeySchema("topic"));
  }

  @DisplayName("obtain last key schema")
  @Test
  void lastKeySchemaWithLastSchema() {

    val res =
        transform.getSchema(
            new SinkRecord("topic", 0, INT32_SCHEMA, 42, BOOLEAN_SCHEMA, true, 1, 0L, CREATE_TIME));

    assertAll(
        "obtained schema",
        () -> assertNotNull(res),
        () -> assertEquals(OPTIONAL_INT32_SCHEMA, transform.lastKeySchema("topic")));
  }

  @DisplayName("obtain last value schema (no last schema)")
  @Test
  void lastValueSchemaWithoutLastSchema() {
    assertEquals(OPTIONAL_STRING_SCHEMA, transform.lastValueSchema("topic"));
  }

  @DisplayName("obtain last value schema")
  @Test
  void lastValueSchemaWithLastSchema() {
    val res =
        transform.getSchema(
            new SinkRecord("topic", 0, INT32_SCHEMA, 42, BOOLEAN_SCHEMA, true, 1, 0L, CREATE_TIME));

    assertAll(
        "obtained schema",
        () -> assertNotNull(res),
        () -> assertEquals(OPTIONAL_BOOLEAN_SCHEMA, transform.lastValueSchema("topic")));
  }

  @DisplayName("apply w/o schema (null key)")
  @Test
  void applyNullKeyWithoutSchema() {
    val res =
        transform.applyWithoutSchema(new SinkRecord("topic", 0, null, null, null, "value", 1));

    assertAll(
        "transformed record",
        () -> assertNotNull(res),
        () -> assertNull(res.valueSchema()),
        () -> assertNull(res.key()),
        () -> assertNotNull(res.value()),
        () -> assertTrue(res.value() instanceof Map));

    @SuppressWarnings("unchecked")
    val value = (Map<String, Object>) res.value();

    assertAll(
        "contained value",
        () -> assertTrue(value.containsKey(TOPIC)),
        () -> assertEquals("topic", value.get(TOPIC)),
        () -> assertTrue(value.containsKey(PARTITION)),
        () -> assertEquals(0, value.get(PARTITION)),
        () -> assertTrue(value.containsKey(OFFSET)),
        () -> assertEquals(1L, value.get(OFFSET)),
        () -> assertTrue(value.containsKey(VALUE)),
        () -> assertEquals("value", value.get(VALUE)),
        () -> assertTrue(value.containsKey(KEY)),
        () -> assertNull(value.get(KEY)));
  }

  @DisplayName("apply w/o schema")
  @Test
  void applyWithoutSchema() {
    val res = transform.applyWithoutSchema(new SinkRecord("topic", 0, null, "key", null, 42, 1));

    assertAll(
        "transformed record",
        () -> assertNotNull(res),
        () -> assertNull(res.valueSchema()),
        () -> assertNotNull(res.key()),
        () -> assertEquals("key", res.key()),
        () -> assertNotNull(res.value()),
        () -> assertTrue(res.value() instanceof Map));

    @SuppressWarnings("unchecked")
    val value = (Map<String, Object>) res.value();

    assertAll(
        "contained value",
        () -> assertTrue(value.containsKey("topic")),
        () -> assertEquals("topic", value.get("topic")),
        () -> assertTrue(value.containsKey("partition")),
        () -> assertEquals(0, value.get("partition")),
        () -> assertTrue(value.containsKey("offset")),
        () -> assertEquals(1L, value.get("offset")),
        () -> assertTrue(value.containsKey("key")),
        () -> assertEquals("key", value.get("key")),
        () -> assertTrue(value.containsKey("value")),
        () -> assertEquals(42, value.get("value")));
  }

  @DisplayName("apply w/ schema (null key)")
  @Test
  void applyNullKeyWithSchema() {
    val res =
        transform.applyWithSchema(
            new SinkRecord(
                "topic", 0, INT32_SCHEMA, null, STRING_SCHEMA, "value", 1, 0L, CREATE_TIME));

    assertAll(
        "transformed record",
        () -> assertNotNull(res),
        () -> assertNotNull(res.valueSchema()),
        () -> assertSame(STRUCT, res.valueSchema().type()),
        () -> assertEquals(OPTIONAL_STRING_SCHEMA, res.valueSchema().field(VALUE).schema()),
        () -> assertEquals(OPTIONAL_INT32_SCHEMA, res.valueSchema().field(KEY).schema()),
        () -> assertNotNull(res.keySchema()),
        () -> assertSame(INT32, res.keySchema().type()),
        () -> assertNull(res.key()),
        () -> assertNotNull(res.value()),
        () -> assertTrue(res.value() instanceof Struct));

    val value = Requirements.requireStruct(res.value(), "testing");

    assertAll(
        "contained value",
        () -> assertEquals("topic", value.getString(TOPIC)),
        () -> assertEquals(0, value.getInt32(PARTITION)),
        () -> assertEquals(1L, value.getInt64(OFFSET)),
        () -> assertEquals("value", value.getString(VALUE)),
        () -> assertNull(value.get(KEY)));
  }

  @DisplayName("apply w/ schema")
  @Test
  void applyWithSchema() {
    val valueSchema =
        SchemaBuilder.struct().field("first", STRING_SCHEMA).field("second", INT32_SCHEMA).build();

    val res =
        transform.applyWithSchema(
            new SinkRecord(
                "topic",
                0,
                STRING_SCHEMA,
                "key",
                valueSchema,
                new Struct(valueSchema).put("first", "first").put("second", 2),
                1,
                0L,
                CREATE_TIME));

    assertAll(
        "transformed record",
        () -> assertNotNull(res),
        () -> assertNotNull(res.keySchema()),
        () -> assertNotNull(res.key()),
        () -> assertEquals("key", res.key()),
        () -> assertNotNull(res.valueSchema()),
        () -> assertSame(STRUCT, res.valueSchema().type()),
        () -> assertNotNull(res.value()),
        () -> assertTrue(res.value() instanceof Struct));

    val value = Requirements.requireStruct(res.value(), "testing");

    assertAll(
        "contained value",
        () -> assertEquals("key", value.getString(KEY)),
        () -> assertEquals("topic", value.getString(TOPIC)),
        () -> assertEquals(0, value.getInt32(PARTITION)),
        () -> assertEquals(1L, value.getInt64(OFFSET)),
        () ->
            assertEquals(
                new Struct(toBuilder(valueSchema).optional().build())
                    .put("first", "first")
                    .put("second", 2),
                value.getStruct("value")));
  }

  @DisplayName("get schema from cache (miss)")
  @Test
  void getSchemaMiss() {

    val valueSchema =
        SchemaBuilder.struct().field("first", STRING_SCHEMA).field("second", INT32_SCHEMA).build();

    val res =
        transform.getSchema(
            new SinkRecord(
                "topic",
                0,
                STRING_SCHEMA,
                "key",
                valueSchema,
                new Struct(valueSchema).put("first", "first").put("second", 2),
                1,
                0L,
                CREATE_TIME));

    assertAll(
        "obtained schema",
        () -> assertNotNull(res),
        () -> assertNotNull(res.field(TOPIC)),
        () -> assertNotNull(res.field(PARTITION)),
        () -> assertNotNull(res.field(OFFSET)),
        () -> assertNotNull(res.field(TIMESTAMP)),
        () -> assertNotNull(res.field(TIMESTAMP_TYPE)),
        () -> assertNotNull(res.field(KEY)),
        () -> assertNotNull(res.field(VALUE)),
        () -> assertNotNull(res.field(HEADERS)),
        () -> assertEquals(SchemaBuilder.string().optional().build(), res.field(KEY).schema()),
        () -> assertEquals(toBuilder(valueSchema).optional().build(), res.field(VALUE).schema()));
  }

  @DisplayName("create schema w/ valid field names")
  @Test
  void createSchemaValidFieldNames() {

    val valueSchema =
        SchemaBuilder.struct().field("first", STRING_SCHEMA).field("second", INT32_SCHEMA).build();

    val res =
        transform.getSchema(
            new SinkRecord(
                "topic",
                0,
                STRING_SCHEMA,
                "key",
                valueSchema,
                new Struct(valueSchema).put("first", "first").put("second", 2),
                1,
                0L,
                CREATE_TIME));

    assertAll(
        "obtained schema",
        () -> assertNotNull(res),
        () -> res.fields().forEach(field -> assertFalse(field.name().contains("."))));
  }

  @DisplayName("get schema from cache (hit)")
  @Test
  void getSchemaHit() {

    val valueSchema =
        SchemaBuilder.struct().field("first", STRING_SCHEMA).field("second", INT32_SCHEMA).build();

    val res =
        transform.getSchema(
            new SinkRecord(
                "topic",
                0,
                STRING_SCHEMA,
                "key",
                valueSchema,
                new Struct(valueSchema).put("first", "first").put("second", 2),
                1,
                0L,
                CREATE_TIME));

    val res2 =
        transform.getSchema(
            new SinkRecord(
                "topic",
                0,
                STRING_SCHEMA,
                "key_alt",
                valueSchema,
                new Struct(valueSchema).put("first", "first_alt").put("second", 3),
                2,
                1L,
                CREATE_TIME));

    assertAll(
        "obtained schema",
        () -> assertNotNull(res),
        () -> assertNotNull(res.field(TOPIC)),
        () -> assertNotNull(res.field(PARTITION)),
        () -> assertNotNull(res.field(OFFSET)),
        () -> assertNotNull(res.field(TIMESTAMP)),
        () -> assertNotNull(res.field(TIMESTAMP_TYPE)),
        () -> assertNotNull(res.field(KEY)),
        () -> assertNotNull(res.field(VALUE)),
        () -> assertNotNull(res.field(HEADERS)),
        () -> assertEquals(SchemaBuilder.string().optional().build(), res.field(KEY).schema()),
        () -> assertEquals(toBuilder(valueSchema).optional().build(), res.field(VALUE).schema()));

    assertAll("obtained schema (on hit)", () -> assertNotNull(res2), () -> assertSame(res, res2));
  }

  @DisplayName("apply with value schema only")
  @Test
  void applyOnlyValueSchema() {
    val res =
        transform.apply(
            new SinkRecord("topic", 0, null, null, STRING_SCHEMA, "value", 1, 0L, CREATE_TIME));

    assertAll(
        "transformed record",
        () -> assertNotNull(res),
        () -> assertNull(res.keySchema()),
        () -> assertNull(res.key()),
        () -> assertNotNull(res.valueSchema()),
        () -> assertEquals(OPTIONAL_STRING_SCHEMA, res.valueSchema().field(VALUE).schema()),
        () -> assertEquals(OPTIONAL_STRING_SCHEMA, res.valueSchema().field(KEY).schema()),
        () -> assertNotNull(res.value()),
        () -> assertTrue(res.value() instanceof Struct));

    val value = Requirements.requireStruct(res.value(), "testing");

    assertAll(
        "contained value",
        () -> assertEquals("topic", value.get(TOPIC)),
        () -> assertEquals(0, value.get(PARTITION)),
        () -> assertEquals(1L, value.get(OFFSET)),
        () -> assertEquals("value", value.get(VALUE)),
        () -> assertNull(value.get(KEY)));
  }

  @DisplayName("apply with key and value schemas")
  @Test
  void applyKeyAndValueSchemas() {
    val valueSchema =
        SchemaBuilder.struct()
            .name("value.schema")
            .field("first", STRING_SCHEMA)
            .field("second", INT32_SCHEMA)
            .build();

    val res =
        transform.apply(
            new SinkRecord(
                "topic",
                0,
                STRING_SCHEMA,
                "key",
                valueSchema,
                new Struct(valueSchema).put("first", "first").put("second", 2),
                1,
                0L,
                CREATE_TIME));

    assertAll(
        "transformed record",
        () -> assertNotNull(res),
        () -> assertNotNull(res.keySchema()),
        () -> assertNotNull(res.key()),
        () -> assertEquals("key", res.key()),
        () -> assertNotNull(res.valueSchema()),
        () -> assertSame(STRUCT, res.valueSchema().type()),
        () -> assertNotNull(res.value()),
        () -> assertTrue(res.value() instanceof Struct));

    val value = Requirements.requireStruct(res.value(), "testing");

    assertAll(
        "contained value",
        () -> assertEquals("key", value.getString(KEY)),
        () -> assertEquals("topic", value.getString(TOPIC)),
        () -> assertEquals(0, value.getInt32(PARTITION)),
        () -> assertEquals(1L, value.getInt64(OFFSET)),
        () ->
            assertEquals(
                new Struct(toBuilder(valueSchema).optional().build())
                    .put("first", "first")
                    .put("second", 2),
                value.getStruct("value")));
  }

  @DisplayName("apply (tombstone)")
  @Test
  void applyTombstone() {
    val valueSchema =
        SchemaBuilder.struct()
            .name("value.schema")
            .field("first", STRING_SCHEMA)
            .field("second", INT32_SCHEMA)
            .build();

    val res =
        transform.apply(
            new SinkRecord(
                "topic", 0, STRING_SCHEMA, "key", valueSchema, null, 1, 0L, CREATE_TIME));

    assertAll(
        "transformed record",
        () -> assertNotNull(res),
        () -> assertNotNull(res.keySchema()),
        () -> assertNotNull(res.key()),
        () -> assertEquals("key", res.key()),
        () -> assertNotNull(res.valueSchema()),
        () -> assertSame(STRUCT, res.valueSchema().type()),
        () -> assertNotNull(res.value()),
        () -> assertTrue(res.value() instanceof Struct));

    val value = Requirements.requireStruct(res.value(), "testing");

    assertAll(
        "contained value",
        () -> assertEquals("key", value.getString(KEY)),
        () -> assertEquals("topic", value.getString(TOPIC)),
        () -> assertEquals(0, value.getInt32(PARTITION)),
        () -> assertEquals(1L, value.getInt64(OFFSET)),
        () -> assertNotNull(value.schema().field(VALUE)),
        () ->
            assertEquals(
                optionalSchemaOrElse(valueSchema, () -> OPTIONAL_STRING_SCHEMA),
                value.schema().field(VALUE).schema()),
        () -> assertNull(value.get(VALUE)));
  }

  @DisplayName("configuration description")
  @Test
  void config() {
    val actual = transform.config();
    assertAll(
        "config defintion",
        () -> assertNotNull(actual),
        () -> assertNotNull(actual.configKeys()),
        () -> assertTrue(actual.configKeys().containsKey(INCLUDE_HEADERS_CONFIG)),
        () -> assertNotNull(actual.defaultValues()),
        () -> assertTrue(actual.defaultValues().containsKey(INCLUDE_HEADERS_CONFIG)),
        () -> assertFalse((Boolean) actual.defaultValues().get(INCLUDE_HEADERS_CONFIG)));
  }

  @DisplayName("configure w/o headers")
  @Test
  void configureWithoutHeaders() {
    assertAll(
        "configuration without headers",
        () -> assertFalse(transform.isIncludeHeaders()),
        () -> assertNotNull(transform.getSchemaUpdateCache()));
  }

  @DisplayName("configure w/ headers")
  @Test
  void testConfigure() {
    val transform = new Wrap<SinkRecord>();
    transform.configure(Collections.singletonMap(INCLUDE_HEADERS_CONFIG, true));

    assertAll(
        "configuration with headers",
        () -> assertTrue(transform.isIncludeHeaders()),
        () -> assertNotNull(transform.getSchemaUpdateCache()));
  }
}
