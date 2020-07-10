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

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;
import lombok.NonNull;
import lombok.Value;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Headers;

/** Helper class for {@link Schema} related tasks. */
public class Schemas {

  /** Wrapper class for unique combinations of schemas. */
  @Value(staticConstructor = "of")
  public static class KeyValueSchema {
    Schema key;
    Schema value;
    Schema headers;
  }

  private Schemas() {
    // unused
  }

  /**
   * Extract a {@link KeyValueSchema} from a provided record of type {@link R}.
   *
   * @param record the record of type {@link R}
   * @param <R> a type extending {@link ConnectRecord}
   * @return the {@link KeyValueSchema} wrapper
   */
  public static <R extends ConnectRecord<R>> KeyValueSchema schemaOf(
      @NonNull R record, @NonNull boolean includeHeaders) {
    return KeyValueSchema.of(
        record.keySchema(),
        record.valueSchema(),
        includeHeaders ? forHeaders(record.headers()) : null);
  }

  public static Schema optionalSchemaOrElse(Schema schema, @NonNull Supplier<Schema> alternative) {
    return Optional.ofNullable(schema)
        .map(Schemas::toBuilder)
        .map(SchemaBuilder::optional)
        .map(SchemaBuilder::build)
        .orElseGet(alternative);
  }

  /**
   * Convert an existing {@link Schema} to a {@link SchemaBuilder}. Note that the contract of this
   * method is that <code>assert Schemas.toBuilder(schema).build().equals(schema);</code>
   *
   * @param schema the {@link Schema} to convert
   * @return the {@link SchemaBuilder}
   */
  public static SchemaBuilder toBuilder(@NonNull Schema schema) {
    SchemaBuilder builder = null;

    // basic initialization based on type
    switch (schema.type()) {
      case ARRAY:
        builder = SchemaBuilder.array(schema.valueSchema());
        break;
      case MAP:
        builder = SchemaBuilder.map(schema.keySchema(), schema.valueSchema());
        break;
      case STRUCT:
        builder = SchemaBuilder.struct();

        for (Field field : schema.fields()) {
          builder.field(field.name(), field.schema());
        }
        break;
      default:
        if (schema.type() != null && schema.type().isPrimitive()) {
          builder = new SchemaBuilder(schema.type());
        }
        break;
    }

    // additional initialization (doc, default, optional)
    if (builder != null) {
      builder = builder.name(schema.name()).version(schema.version()).doc(schema.doc());

      if (schema instanceof ConnectSchema) {
        ConnectSchema connectSchema = (ConnectSchema) schema;
        Optional<Map<String, String>> params = Optional.ofNullable(connectSchema.parameters());
        builder = builder.parameters(params.orElseGet(Collections::emptyMap));
      }

      if (schema.defaultValue() != null) {
        builder = builder.defaultValue(schema.defaultValue());
      }

      if (schema.isOptional()) {
        return builder.optional();
      }
    }

    return builder;
  }

  /**
   * Returns a schema for storing the message headers
   *
   * @param headers
   * @return
   */
  public static Schema forHeaders(Headers headers) {
    final Map<String, List<Schema>> schemas = new HashMap<>();
    StreamSupport.stream(headers.spliterator(), false)
        .forEach(
            header -> {
              if (!schemas.containsKey(header.key())) {
                schemas.put(header.key(), new ArrayList<>());
              }
              schemas.get(header.key()).add(header.schema());
            });
    final SchemaBuilder builder = SchemaBuilder.struct();
    schemas.forEach((key, value) -> builder.field(key, mergeSchemas(value)));
    return builder.build();
  }

  private static Schema mergeSchemas(List<Schema> headers) {
    Schema schema = null;
    for (Schema headerSchema : headers) {
      schema = mergeSchemas(schema, headerSchema);
    }
    return SchemaBuilder.array(schema).optional().build();
  }

  private static Schema mergeSchemas(Schema schema, Schema addition) {
    if (schema == null) {
      return addition;
    }
    switch (addition.type()) {
      case FLOAT32:
      case FLOAT64:
        if (schema.type().equals(Schema.Type.FLOAT32)
            || schema.type().equals(Schema.Type.FLOAT64)) {
          return Schema.OPTIONAL_FLOAT64_SCHEMA;
        }
      case INT8:
      case INT16:
      case INT32:
      case INT64:
        if (Arrays.asList(Schema.Type.INT8, Schema.Type.INT16, Schema.Type.INT32, Schema.Type.INT64)
            .contains(schema.type())) {
          return Schema.OPTIONAL_INT64_SCHEMA;
        }
      case STRUCT:
        if (schema.type() == Schema.Type.STRUCT) {
          return mergeStructs(schema, addition);
        }
      case MAP:
        if (schema.type() == Schema.Type.MAP) {
          return SchemaBuilder.map(
                  mergeSchemas(schema.keySchema(), addition.keySchema()),
                  mergeSchemas(schema.valueSchema(), addition.valueSchema()))
              .optional()
              .build();
        }
      case ARRAY:
        if (schema.type() == Schema.Type.MAP) {
          return SchemaBuilder.array(mergeSchemas(schema.valueSchema(), addition.valueSchema()))
              .optional()
              .build();
        }
      default:
        if (Objects.equals(schema.type(), addition.type())) {
          return toBuilder(addition).optional().build();
        }
    }
    throw new DataException(
        "Cannot merge incompatible schemas of type '"
            + schema.type().getName()
            + "' and '"
            + addition.type().getName()
            + "'.");
  }

  private static Schema mergeStructs(Schema a, Schema b) {
    SchemaBuilder builder = SchemaBuilder.struct();
    applyStructToBuilder(builder, a);
    applyStructToBuilder(builder, b);
    return builder.optional().build();
  }

  private static void applyStructToBuilder(SchemaBuilder builder, Schema schema) {
    for (Field field : schema.fields()) {
      Field existingField = builder.field(field.name());
      if (existingField != null) {
        builder.field(field.name(), mergeSchemas(existingField.schema(), field.schema()));
      } else {
        builder.field(field.name(), field.schema());
      }
    }
  }
}
