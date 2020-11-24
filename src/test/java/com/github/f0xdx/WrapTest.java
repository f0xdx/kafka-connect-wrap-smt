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
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class WrapTest {
  private static final String TOPIC_VAL = "topic";
  private static final Integer PARTITION_VAL = 0;
  private static final Long OFFSET_VAL = 1L;
  private static final TimestampType TIMESTAMP_TYPE_VAL = CREATE_TIME;
  private static final Long TIMESTAMP_VAL = 123456789L;

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

  @DisplayName("apply w/o schema (null key)")
  @Test
  void applyNullKeyWithoutSchema() {
    val res = applyRecord(null, null, null, "value");

    assertAll("transformed record", () -> assertNull(res.key()));

    val value = assertCommonMap(res);

    assertAll(
        "contained value",
        () -> assertTrue(value.containsKey(VALUE)),
        () -> assertEquals("value", value.get(VALUE)),
        () -> assertTrue(value.containsKey(KEY)),
        () -> assertNull(value.get(KEY)));
  }

  @DisplayName("apply w/o schema")
  @Test
  void applyWithoutSchema() {
    val res = applyRecord(null, "key", null, 42);

    assertAll(
        "transformed record", () -> assertNotNull(res.key()), () -> assertEquals("key", res.key()));

    val value = assertCommonMap(res);

    assertAll(
        "contained value",
        () -> assertTrue(value.containsKey("key")),
        () -> assertEquals("key", value.get("key")),
        () -> assertTrue(value.containsKey("value")),
        () -> assertEquals(42, value.get("value")));
  }

  @DisplayName("apply w/ schema (null key)")
  @Test
  void applyNullKeyWithSchema() {
    val res = applyRecord(INT32_SCHEMA, null, STRING_SCHEMA, "value");

    assertAll(
        "transformed record",
        () -> assertEquals(OPTIONAL_STRING_SCHEMA, res.valueSchema().field(VALUE).schema()),
        () -> assertEquals(OPTIONAL_INT32_SCHEMA, res.valueSchema().field(KEY).schema()),
        () -> assertNotNull(res.keySchema()),
        () -> assertSame(INT32, res.keySchema().type()),
        () -> assertNull(res.key()));

    val value = assertCommonStruct(res);

    assertAll(
        "contained value",
        () -> assertEquals("value", value.getString(VALUE)),
        () -> assertNull(value.get(KEY)));
  }

  @DisplayName("apply w/ schema")
  @Test
  void applyWithSchema() {
    val valueSchema =
        SchemaBuilder.struct().field("first", STRING_SCHEMA).field("second", INT32_SCHEMA).build();
    val value = new Struct(valueSchema).put("first", "first").put("second", 2);
    val res = applyRecord(STRING_SCHEMA, "key", valueSchema, value);

    assertAll(
        "transformed record",
        () -> assertNotNull(res.keySchema()),
        () -> assertNotNull(res.key()),
        () -> assertEquals("key", res.key()));

    val transformedValue = assertCommonStruct(res);

    assertAll(
        "contained value",
        () -> assertEquals("key", transformedValue.getString(KEY)),
        () ->
            assertEquals(
                new Struct(toBuilder(valueSchema).optional().build())
                    .put("first", "first")
                    .put("second", 2),
                transformedValue.getStruct("value")));
  }

  @DisplayName("apply with value schema only")
  @Test
  void applyOnlyValueSchema() {
    val res = applyRecord(null, null, STRING_SCHEMA, "value");

    assertAll(
        "transformed record",
        () -> assertNull(res.keySchema()),
        () -> assertNull(res.key()),
        () -> assertEquals(OPTIONAL_STRING_SCHEMA, res.valueSchema().field(VALUE).schema()),
        () -> assertEquals(OPTIONAL_STRING_SCHEMA, res.valueSchema().field(KEY).schema()));

    val value = assertCommonStruct(res);

    assertAll(
        "contained value",
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
    val value = new Struct(valueSchema).put("first", "first").put("second", 2);

    val res = applyRecord(STRING_SCHEMA, "key", valueSchema, value);

    assertAll(
        "transformed record",
        () -> assertNotNull(res.keySchema()),
        () -> assertNotNull(res.key()),
        () -> assertEquals("key", res.key()));

    val transformedValue = assertCommonStruct(res);

    assertAll(
        "contained value",
        () -> assertEquals("key", transformedValue.getString(KEY)),
        () ->
            assertEquals(
                new Struct(toBuilder(valueSchema).optional().build())
                    .put("first", "first")
                    .put("second", 2),
                transformedValue.getStruct("value")));
  }

  @DisplayName("apply (tombstone)")
  @Test
  void applyTombstone() {
    val res = applyRecord(STRING_SCHEMA, "key", null, null);

    assertAll(
        "transformed record",
        () -> assertNotNull(res.keySchema()),
        () -> assertNotNull(res.key()),
        () -> assertEquals("key", res.key()));

    val value = assertCommonStruct(res);

    assertAll(
        "contained value",
        () -> assertEquals("key", value.getString(KEY)),
        () -> assertNotNull(value.schema().field(VALUE)),
        () -> assertEquals(OPTIONAL_STRING_SCHEMA, value.schema().field(VALUE).schema()),
        () -> assertNull(value.get(VALUE)));
  }

  @DisplayName("apply (null record)")
  @Test
  void applyNullRecord() {
    val res = transform.apply(null);
    assertNull(res);
  }

  @DisplayName("get schema from cache (miss)")
  @Test
  void getSchemaMiss() {
    val valueSchema =
        SchemaBuilder.struct().field("first", STRING_SCHEMA).field("second", INT32_SCHEMA).build();
    val value = new Struct(valueSchema).put("first", "first").put("second", 2);
    val record = newRecord(STRING_SCHEMA, "key", valueSchema, value);

    val res = transform.getSchema(record);
    assertCommonSchema(
        res, SchemaBuilder.string().optional().build(), toBuilder(valueSchema).optional().build());
  }

  @DisplayName("get schema from cache (hit)")
  @Test
  void getSchemaHit() {
    val valueSchema =
        SchemaBuilder.struct().field("first", STRING_SCHEMA).field("second", INT32_SCHEMA).build();
    val value = new Struct(valueSchema).put("first", "first").put("second", 2);
    val record = newRecord(STRING_SCHEMA, "key", valueSchema, value);
    val value2 = new Struct(valueSchema).put("first", "first_alt").put("second", 3);
    val record2 = newRecord(STRING_SCHEMA, "key_alt", valueSchema, value2);

    val res = transform.getSchema(record);
    val res2 = transform.getSchema(record2);

    assertCommonSchema(
        res, SchemaBuilder.string().optional().build(), toBuilder(valueSchema).optional().build());
    assertAll("obtained schema (on hit)", () -> assertNotNull(res2), () -> assertSame(res, res2));
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
        () -> assertNotNull(transform.getSchemaUpdateCache()),
        () -> assertNotNull(transform.getTopicSchemaCache()));
  }

  @DisplayName("configure w/ headers")
  @Test
  void testConfigure() {
    val transform = new Wrap<SinkRecord>();
    transform.configure(Collections.singletonMap(INCLUDE_HEADERS_CONFIG, true));

    assertAll(
        "configuration with headers",
        () -> assertTrue(transform.isIncludeHeaders()),
        () -> assertNotNull(transform.getSchemaUpdateCache()),
        () -> assertNotNull(transform.getTopicSchemaCache()));
  }

  @DisplayName("close")
  @Test
  void testClose() {
    assertAll(
        "configuration successful",
        () -> assertNotNull(transform.getSchemaUpdateCache()),
        () -> assertNotNull(transform.getTopicSchemaCache()));

    transform.close();

    assertAll(
        "configuration successful",
        () -> assertNull(transform.getSchemaUpdateCache()),
        () -> assertNull(transform.getTopicSchemaCache()));
  }

  private SinkRecord newRecord(Schema keySchema, Object key, Schema valueSchema, Object value) {
    return new SinkRecord(
        TOPIC_VAL,
        PARTITION_VAL,
        keySchema,
        key,
        valueSchema,
        value,
        OFFSET_VAL,
        TIMESTAMP_VAL,
        TIMESTAMP_TYPE_VAL);
  }

  private SinkRecord applyRecord(Schema keySchema, Object key, Schema valueSchema, Object value) {
    return transform.apply(newRecord(keySchema, key, valueSchema, value));
  }

  private Map<String, Object> assertCommonMap(SinkRecord res) {
    assertAll(
        "is common map record",
        () -> assertNotNull(res),
        () -> assertNull(res.valueSchema()),
        () -> assertNotNull(res.value()),
        () -> assertTrue(res.value() instanceof Map));

    @SuppressWarnings("unchecked")
    val value = (Map<String, Object>) res.value();

    assertAll(
        "contains common values",
        () -> assertTrue(value.containsKey(TOPIC)),
        () -> assertEquals(TOPIC_VAL, value.get(TOPIC)),
        () -> assertTrue(value.containsKey(PARTITION)),
        () -> assertEquals(PARTITION_VAL, value.get(PARTITION)),
        () -> assertTrue(value.containsKey(OFFSET)),
        () -> assertEquals(OFFSET_VAL, value.get(OFFSET)),
        () -> assertTrue(value.containsKey(TIMESTAMP)),
        () -> assertEquals(TIMESTAMP_VAL, value.get(TIMESTAMP)),
        () -> assertTrue(value.containsKey(TIMESTAMP_TYPE)),
        () -> assertEquals(TIMESTAMP_TYPE_VAL.name, value.get(TIMESTAMP_TYPE)));

    return value;
  }

  private Struct assertCommonStruct(SinkRecord res) {
    assertAll(
        "is common struct record",
        () -> assertNotNull(res),
        () -> assertNotNull(res.valueSchema()),
        () -> assertSame(STRUCT, res.valueSchema().type()),
        () -> assertNotNull(res.value()),
        () -> assertTrue(res.value() instanceof Struct));

    val value = (Struct) res.value();

    assertAll(
        "contains common values",
        () -> assertEquals(TOPIC_VAL, value.getString(TOPIC)),
        () -> assertEquals(PARTITION_VAL, value.getInt32(PARTITION)),
        () -> assertEquals(OFFSET_VAL, value.getInt64(OFFSET)),
        () -> assertEquals(TIMESTAMP_VAL, value.getInt64(TIMESTAMP)),
        () -> assertEquals(TIMESTAMP_TYPE_VAL.name, value.getString(TIMESTAMP_TYPE)));

    return value;
  }

  private void assertCommonSchema(Schema schema, Schema keySchema, Schema valueSchema) {
    assertAll(
        "obtained schema",
        () -> assertNotNull(schema),
        () -> assertNotNull(schema.field(TOPIC)),
        () -> assertNotNull(schema.field(PARTITION)),
        () -> assertNotNull(schema.field(OFFSET)),
        () -> assertNotNull(schema.field(TIMESTAMP)),
        () -> assertNotNull(schema.field(TIMESTAMP_TYPE)),
        () -> assertNotNull(schema.field(KEY)),
        () -> assertNotNull(schema.field(VALUE)),
        () -> assertNull(schema.field(HEADERS)),
        () -> assertEquals(keySchema, schema.field(KEY).schema()),
        () -> assertEquals(valueSchema, schema.field(VALUE).schema()),
        () -> schema.fields().forEach(field -> assertFalse(field.name().contains("."))));
  }
}
