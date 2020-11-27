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

import lombok.NonNull;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;

import java.util.*;

import static com.github.f0xdx.Schemas.toBuilder;

/** Helper class to merge different schemas together. */
class Merger {
  private static final Map<Type, Map<Type, Schema>> mergeMap = new HashMap<>();

  /*
   * Build a map of mergeable types and the result of a merge.
   */
  static {
    Type[] floatTypes = {Type.FLOAT32, Type.FLOAT64};
    Schema[] floatSchemas = {Schema.OPTIONAL_FLOAT32_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA};
    Type[] intTypes = {Type.INT8, Type.INT16, Type.INT32, Type.INT64};
    Schema[] intSchemas = {
      Schema.OPTIONAL_INT8_SCHEMA,
      Schema.OPTIONAL_INT16_SCHEMA,
      Schema.OPTIONAL_INT32_SCHEMA,
      Schema.OPTIONAL_INT64_SCHEMA
    };
    for (int i = 0; i < floatTypes.length; i++) {
      for (int j = 0; j < floatTypes.length; j++) {
        mergeMap
            .computeIfAbsent(floatTypes[i], (t) -> new HashMap<>())
            .put(floatTypes[j], floatSchemas[Math.max(i, j)]);
      }
    }
    for (int i = 0; i < intTypes.length; i++) {
      for (int j = 0; j < intTypes.length; j++) {
        mergeMap
            .computeIfAbsent(intTypes[i], (t) -> new HashMap<>())
            .put(intTypes[j], intSchemas[Math.max(i, j)]);
      }
    }
  }

  private Merger() {}

  static Schema mergeHeaderSchemas(@NonNull List<Schema> headers) {
    return headers.stream()
        .reduce(Merger::mergeSchemas)
        .map(SchemaBuilder::array)
        .map(SchemaBuilder::optional)
        .map(SchemaBuilder::build)
        .orElseThrow(AssertionError::new);
  }

  static Schema mergeSchemas(@NonNull Schema schema, @NonNull Schema addition) {
    if (schema.equals(addition)) {
      return schema;
    }

    if (mergeMap.containsKey(schema.type())
        && mergeMap.get(schema.type()).containsKey(addition.type())) {
      return mergeMap.get(schema.type()).get(addition.type());
    }

    switch (addition.type()) {
      case STRUCT:
        if (schema.type() == Type.STRUCT) {
          return mergeStructs(schema, addition);
        }
        break;
      case MAP:
        if (schema.type() == Type.MAP) {
          return SchemaBuilder.map(
                  mergeSchemas(schema.keySchema(), addition.keySchema()),
                  mergeSchemas(schema.valueSchema(), addition.valueSchema()))
              .optional()
              .build();
        }
        break;
      case ARRAY:
        if (schema.type() == Type.ARRAY) {
          return SchemaBuilder.array(mergeSchemas(schema.valueSchema(), addition.valueSchema()))
              .optional()
              .build();
        }
        break;
      default:
        if (Objects.equals(schema.type(), addition.type())) {
          return Optional.ofNullable(toBuilder(addition))
              .map(SchemaBuilder::optional)
              .map(SchemaBuilder::build)
              .orElseThrow(() -> new DataException("Cannot derive builder for addition schema"));
        }
        break;
    }
    throw new DataException(
        "Cannot merge incompatible schemas of type '"
            + schema.type().getName()
            + "' and '"
            + addition.type().getName()
            + "'.");
  }

  private static Schema mergeStructs(@NonNull Schema a, @NonNull Schema b) {
    final SchemaBuilder builder = SchemaBuilder.struct();
    final Map<String, Schema> fields = new HashMap<>();
    applyStructToBuilder(fields, a);
    applyStructToBuilder(fields, b);
    fields.forEach(builder::field);
    return builder.optional().build();
  }

  private static void applyStructToBuilder(
      @NonNull Map<String, Schema> fields, @NonNull Schema schema) {
    for (Field field : schema.fields()) {
      final Schema existing = fields.get(field.name());
      if (existing != null) {
        fields.put(field.name(), mergeSchemas(existing, field.schema()));
      } else {
        fields.put(field.name(), field.schema());
      }
    }
  }
}
