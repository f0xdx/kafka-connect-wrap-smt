package com.github.f0xdx;

import static org.apache.kafka.connect.data.Schema.BOOLEAN_SCHEMA;
import static org.apache.kafka.connect.data.Schema.INT16_SCHEMA;
import static org.apache.kafka.connect.data.Schema.INT32_SCHEMA;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import lombok.val;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

class SchemasTest {

  @DisplayName("key/value schema of (null value)")
  @SuppressWarnings("ConstantConditions")
  @Test
  void schemaOfNull() {
    assertTrue(
        assertThrows(
            NullPointerException.class,
            () -> Schemas.schemaOf(null)
        ).getMessage().contains("record is marked non-null")
    );
  }

  @DisplayName("key/value schema of")
  @Test
  void schemaOf() {
    val record = new SinkRecord(
        "topic",
        0,
        STRING_SCHEMA,
        "key",
        BOOLEAN_SCHEMA,
        true,
        0
    );

    val actual = Schemas.schemaOf(record);

    assertAll(
        "schema of",
        () -> assertEquals(STRING_SCHEMA, actual.getKey()),
        () -> assertEquals(BOOLEAN_SCHEMA, actual.getValue())
    );
  }

  @DisplayName("to builder (null value)")
  @SuppressWarnings("ConstantConditions")
  @Test
  void toBuilderNull() {
    assertTrue(
        assertThrows(
            NullPointerException.class,
            () -> Schemas.toBuilder(null)
        ).getMessage().contains("schema is marked non-null")
    );
  }

  @DisplayName("to builder (primitive types)")
  @ParameterizedTest
  @EnumSource(
      value = Type.class,
      names = {"ARRAY", "MAP", "STRUCT"},
      mode = Mode.EXCLUDE
  )
  void toBuilderPrimitive(Type type) {
    val schema = new SchemaBuilder(type).doc("foo").version(1).build();
    val optionalSchema = new SchemaBuilder(type).optional().build();

    assertAll(
        "schema builder from primitive",
        () -> assertEquals(schema, Schemas.toBuilder(schema).build()),
        () -> assertEquals(optionalSchema, Schemas.toBuilder(optionalSchema).build())
    );
  }

  @DisplayName("to builder (default value)")
  @Test
  void toBuilderDefault() {
    val schema = SchemaBuilder.string().defaultValue("foo").build();
    val result = Schemas.toBuilder(schema);

    assertAll(
        "schema builder from string (default = foo)",
        () -> assertNotNull(result),
        () -> assertEquals(schema, result.build())
    );
  }

  @DisplayName("to builder (array)")
  @Test
  void toBuilderArray() {
    val schema = SchemaBuilder.array(STRING_SCHEMA).build();
    val result = Schemas.toBuilder(schema);
    assertAll(
        "schema builder from array",
        () -> assertNotNull(result),
        () -> assertEquals(schema, result.build())
    );
  }

  @DisplayName("to builder (map)")
  @Test
  void toBuilderMap() {
    val schema = SchemaBuilder.map(STRING_SCHEMA, INT32_SCHEMA).build();
    val result = Schemas.toBuilder(schema);
    assertAll(
        "schema builder from map",
        () -> assertNotNull(result),
        () -> assertEquals(schema, result.build())
    );
  }

  @DisplayName("to builder (struct)")
  @Test
  void toBuilderStruct() {
    val schema = SchemaBuilder.struct()
        .field("one", STRING_SCHEMA)
        .field(
            "two",
            SchemaBuilder.struct()
                .field("three", INT32_SCHEMA)
                .build()
        )
        .build();

    val result = Schemas.toBuilder(schema);
    assertAll(
        "schema builder from map",
        () -> assertNotNull(result),
        () -> assertEquals(schema, result.build())
    );
  }
}