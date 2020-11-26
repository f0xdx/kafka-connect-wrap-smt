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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class MergerTest {
  private static Type[][] simpleSchemaHappyCases =
      new Type[][] {
        new Type[] {Type.FLOAT32, Type.FLOAT32, Type.FLOAT32},
        new Type[] {Type.FLOAT32, Type.FLOAT64, Type.FLOAT64},
        new Type[] {Type.INT8, Type.INT8, Type.INT8},
        new Type[] {Type.INT8, Type.INT16, Type.INT16},
        new Type[] {Type.INT32, Type.INT8, Type.INT32},
        new Type[] {Type.INT32, Type.INT8, Type.INT32},
        new Type[] {Type.INT32, Type.INT64, Type.INT64},
        new Type[] {Type.BOOLEAN, Type.BOOLEAN, Type.BOOLEAN},
        new Type[] {Type.STRING, Type.STRING, Type.STRING},
      };
  private static Type[][] simpleSchemaFailingCases =
      new Type[][] {
        new Type[] {Type.FLOAT32, Type.INT32},
        new Type[] {Type.FLOAT32, Type.STRING},
        new Type[] {Type.FLOAT32, Type.STRUCT},
        new Type[] {Type.STRING, Type.ARRAY},
        new Type[] {Type.BOOLEAN, Type.INT8},
      };

  @Test
  @DisplayName("Simple schema merging works")
  void testMergeSimpleSchemas() {
    // Test quick return on equality
    Schema result = Merger.mergeSchemas(Schema.OPTIONAL_INT8_SCHEMA, Schema.OPTIONAL_INT8_SCHEMA);
    assertSame(result, Schema.OPTIONAL_INT8_SCHEMA);

    // Test normal merging
    for (Type[] test : simpleSchemaHappyCases) {
      testSimpleHappyCase(test[0], test[1], test[2]);
    }
    for (Type[] test : simpleSchemaFailingCases) {
      testSimpleFailingCase(test[0], test[1]);
    }
  }

  @Test
  @DisplayName("Struct schema merging works")
  void testMergeStructs() {
    Schema a1 = SchemaBuilder.struct().field("a", Schema.OPTIONAL_STRING_SCHEMA).build();
    Schema a2 = SchemaBuilder.struct().field("b", Schema.OPTIONAL_STRING_SCHEMA).build();
    Schema expectedA =
        SchemaBuilder.struct()
            .field("a", Schema.OPTIONAL_STRING_SCHEMA)
            .field("b", Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build();
    assertEquals(expectedA, Merger.mergeSchemas(a1, a2));

    Schema b1 = SchemaBuilder.struct().field("a", Schema.OPTIONAL_INT64_SCHEMA).build();
    Schema b2 = SchemaBuilder.struct().field("a", Schema.OPTIONAL_INT8_SCHEMA).build();
    Schema expectedB =
        SchemaBuilder.struct().field("a", Schema.OPTIONAL_INT64_SCHEMA).optional().build();
    assertEquals(expectedB, Merger.mergeSchemas(b1, b2));

    Schema sub = SchemaBuilder.struct().field("a", Schema.OPTIONAL_INT64_SCHEMA).build();
    Schema c1 = SchemaBuilder.struct().field("a", sub).build();
    Schema c2 = SchemaBuilder.struct().field("b", Schema.OPTIONAL_STRING_SCHEMA).build();
    Schema expectedC =
        SchemaBuilder.struct()
            .field("a", sub)
            .field("b", Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build();
    assertEquals(expectedC, Merger.mergeSchemas(c1, c2));

    Schema d1 = SchemaBuilder.struct().field("a", Schema.OPTIONAL_INT8_SCHEMA).build();
    Schema d2 = SchemaBuilder.struct().field("a", Schema.OPTIONAL_STRING_SCHEMA).build();
    assertThrows(DataException.class, () -> Merger.mergeSchemas(d1, d2));
  }

  @Test
  @DisplayName("Array schema merging works")
  void testMergeArrays() {
    Schema struct1 = SchemaBuilder.struct().field("a", Schema.OPTIONAL_STRING_SCHEMA).build();
    Schema struct2 = SchemaBuilder.struct().field("b", Schema.OPTIONAL_STRING_SCHEMA).build();
    Schema a1 = SchemaBuilder.array(struct1);
    Schema a2 = SchemaBuilder.array(struct2);
    Schema expectedStruct =
        SchemaBuilder.struct()
            .field("a", Schema.OPTIONAL_STRING_SCHEMA)
            .field("b", Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build();
    Schema expectedA = SchemaBuilder.array(expectedStruct).optional().build();
    assertEquals(expectedA, Merger.mergeSchemas(a1, a2));

    Schema b1 = SchemaBuilder.array(Schema.OPTIONAL_INT8_SCHEMA).build();
    Schema b2 = SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).build();
    Schema expectedB = SchemaBuilder.array(Schema.OPTIONAL_INT64_SCHEMA).optional().build();
    assertEquals(expectedB, Merger.mergeSchemas(b1, b2));

    Schema c1 = SchemaBuilder.array(Schema.OPTIONAL_INT8_SCHEMA).build();
    Schema c2 = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build();
    assertThrows(DataException.class, () -> Merger.mergeSchemas(c1, c2));
  }

  @Test
  @DisplayName("Array schema merging works")
  void testMergeMaps() {
    Schema struct1 = SchemaBuilder.struct().field("a", Schema.OPTIONAL_STRING_SCHEMA).build();
    Schema struct2 = SchemaBuilder.struct().field("b", Schema.OPTIONAL_STRING_SCHEMA).build();
    Schema a1 = SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, struct1);
    Schema a2 = SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, struct2);
    Schema expectedStruct =
        SchemaBuilder.struct()
            .field("a", Schema.OPTIONAL_STRING_SCHEMA)
            .field("b", Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build();
    Schema expectedA =
        SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, expectedStruct).optional().build();
    assertEquals(expectedA, Merger.mergeSchemas(a1, a2));

    Schema b1 =
        SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT8_SCHEMA).build();
    Schema b2 =
        SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA).build();
    Schema expectedB =
        SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA)
            .optional()
            .build();
    assertEquals(expectedB, Merger.mergeSchemas(b1, b2));

    Schema c1 =
        SchemaBuilder.map(Schema.OPTIONAL_FLOAT32_SCHEMA, Schema.OPTIONAL_INT8_SCHEMA).build();
    Schema c2 =
        SchemaBuilder.map(Schema.OPTIONAL_FLOAT64_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA).build();
    Schema expectedC =
        SchemaBuilder.map(Schema.OPTIONAL_FLOAT64_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA)
            .optional()
            .build();
    assertEquals(expectedC, Merger.mergeSchemas(c1, c2));

    Schema d1 =
        SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT8_SCHEMA).build();
    Schema d2 =
        SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).build();
    assertThrows(DataException.class, () -> Merger.mergeSchemas(d1, d2));
  }

  @Test
  @DisplayName("Header schema merging works")
  void testMergeHeaderSchemas() {
    Schema a = Schema.OPTIONAL_INT8_SCHEMA;
    Schema b = Schema.OPTIONAL_INT16_SCHEMA;
    Schema c = Schema.OPTIONAL_INT32_SCHEMA;
    Schema expected = SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build();
    assertEquals(expected, Merger.mergeHeaderSchemas(Arrays.asList(a, b, c)));

    assertThrows(
        DataException.class,
        () ->
            Merger.mergeHeaderSchemas(Arrays.asList(Schema.BOOLEAN_SCHEMA, Schema.STRING_SCHEMA)));
  }

  private void testSimpleHappyCase(Type a, Type b, Type expected) {
    Schema schemaA = buildSimpleSchema(a, "a");
    Schema schemaB = buildSimpleSchema(b, "b");
    Schema result = Merger.mergeSchemas(schemaA, schemaB);
    assertEquals(expected, result.type());
    assertTrue(result.isOptional());
  }

  private void testSimpleFailingCase(Type a, Type b) {
    Schema schemaA = buildSimpleSchema(a, "a");
    Schema schemaB = buildSimpleSchema(b, "b");
    assertThrows(DataException.class, () -> Merger.mergeSchemas(schemaA, schemaB));
  }

  private Schema buildSimpleSchema(Type type, String doc) {
    return SchemaBuilder.type(type).doc(doc).optional().build();
  }
}
