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

import static org.apache.kafka.connect.data.Schema.*;
import static org.junit.jupiter.api.Assertions.*;

import lombok.val;
import org.apache.kafka.connect.data.Schema.*;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

class SchemasTest {

  @DisplayName("cacheKey of (null value)")
  @SuppressWarnings("ConstantConditions")
  @Test
  void cacheKeyNull() {
    assertTrue(
        assertThrows(NullPointerException.class, () -> Schemas.cacheKey(null, false))
            .getMessage()
            .contains("record is marked non-null"));
  }

  @DisplayName("cacheKey of")
  @Test
  void cacheKey() {
    val record = new SinkRecord("topic", 0, STRING_SCHEMA, "key", BOOLEAN_SCHEMA, true, 0);

    val actual = Schemas.cacheKey(record, false);

    assertAll(
        "schema of",
        () -> assertEquals(STRING_SCHEMA, actual.getKey()),
        () -> assertEquals(BOOLEAN_SCHEMA, actual.getValue()));
  }

  @DisplayName("derive optional schema")
  @ParameterizedTest
  @EnumSource(
      value = Type.class,
      names = {"ARRAY", "MAP"},
      mode = Mode.EXCLUDE)
  void optionalSchemaOrElse(Type type) {
    val schema = new SchemaBuilder(type).build();
    val optionalSchema = new SchemaBuilder(type).optional().build();

    assertAll(
        "optional schema",
        () -> assertEquals(optionalSchema, Schemas.optionalSchemaOrElse(schema, () -> null)),
        () ->
            assertEquals(optionalSchema, Schemas.optionalSchemaOrElse(optionalSchema, () -> null)));
  }

  @Test
  void optionalSchemaOrElseNull() {
    assertEquals(
        OPTIONAL_STRING_SCHEMA, Schemas.optionalSchemaOrElse(null, () -> OPTIONAL_STRING_SCHEMA));
  }

  @DisplayName("to builder (null value)")
  @SuppressWarnings("ConstantConditions")
  @Test
  void toBuilderNull() {
    assertTrue(
        assertThrows(NullPointerException.class, () -> Schemas.toBuilder(null))
            .getMessage()
            .contains("schema is marked non-null"));
  }

  @DisplayName("to builder (primitive types)")
  @ParameterizedTest
  @EnumSource(
      value = Type.class,
      names = {"ARRAY", "MAP", "STRUCT"},
      mode = Mode.EXCLUDE)
  void toBuilderPrimitive(Type type) {
    val schema = new SchemaBuilder(type).doc("foo").version(1).build();
    val optionalSchema = new SchemaBuilder(type).optional().build();

    assertAll(
        "schema builder from primitive",
        () -> assertEquals(schema, Schemas.toBuilder(schema).build()),
        () -> assertEquals(optionalSchema, Schemas.toBuilder(optionalSchema).build()));
  }

  @DisplayName("to builder (default value)")
  @Test
  void toBuilderDefault() {
    val schema = SchemaBuilder.string().defaultValue("foo").build();
    val result = Schemas.toBuilder(schema);

    assertAll(
        "schema builder from string (default = foo)",
        () -> assertNotNull(result),
        () -> assertEquals(schema, result.build()));
  }

  @DisplayName("to builder (array)")
  @Test
  void toBuilderArray() {
    val schema = SchemaBuilder.array(STRING_SCHEMA).name("an.array").build();
    val result = Schemas.toBuilder(schema);
    assertAll(
        "schema builder from array",
        () -> assertNotNull(result),
        () -> assertEquals(schema, result.build()));
  }

  @DisplayName("to builder (map)")
  @Test
  void toBuilderMap() {
    val schema = SchemaBuilder.map(STRING_SCHEMA, INT32_SCHEMA).name("a.map").build();
    val result = Schemas.toBuilder(schema);
    assertAll(
        "schema builder from map",
        () -> assertNotNull(result),
        () -> assertEquals(schema, result.build()));
  }

  @DisplayName("to builder (struct)")
  @Test
  void toBuilderStruct() {
    val schema =
        SchemaBuilder.struct()
            .name("a.struct")
            .field("one", STRING_SCHEMA)
            .field("two", SchemaBuilder.struct().field("three", INT32_SCHEMA).build())
            .build();

    val result = Schemas.toBuilder(schema);
    assertAll(
        "schema builder from map",
        () -> assertNotNull(result),
        () -> assertEquals(schema, result.build()));
  }

  @DisplayName("schema with parameters")
  @Test
  void schemaWithParameters() {
    val schema =
        SchemaBuilder.struct()
            .name("d.struct")
            .field("str", STRING_SCHEMA)
            .parameter("connect.doc", "documentation here")
            .build();

    val result = Schemas.toBuilder(schema);
    assertAll(
        "schema builder with parameters",
        () -> assertNotNull(result),
        () -> assertEquals(schema.parameters().size(), result.build().parameters().size()),
        () -> assertTrue(result.build().parameters().containsKey("connect.doc")),
        () -> assertEquals("documentation here", result.build().parameters().get("connect.doc")));
  }
}
