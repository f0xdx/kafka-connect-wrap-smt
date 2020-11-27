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
import lombok.Value;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.header.Headers;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

/** Helper class for {@link Schema} related tasks. */
class Schemas {
  /** Wrapper class for unique combinations of schemas. */
  @Value(staticConstructor = "of")
  public static class SchemaCacheKey {
    Schema key;
    Schema value;
    Map<String, List<Schema>> headers;
  }

  private Schemas() {
    // unused
  }

  /**
   * Extract a {@link SchemaCacheKey} from a provided record of type {@link R}.
   *
   * @param record the record of type {@link R}
   * @param includeHeaders whether to include headers into the schema
   * @param <R> a type extending {@link ConnectRecord}
   * @return the {@link SchemaCacheKey} wrapper
   */
  static <R extends ConnectRecord<R>> SchemaCacheKey cacheKey(
      @NonNull R record, boolean includeHeaders) {
    return SchemaCacheKey.of(
        record.keySchema(),
        record.valueSchema(),
        includeHeaders ? cacheKey(record.headers()) : null);
  }

  static Map<String, List<Schema>> cacheKey(Headers headers) {
    final Map<String, List<Schema>> key = new HashMap<>();
    headers.forEach(h -> key.computeIfAbsent(h.key(), k -> new ArrayList<>()).add(h.schema()));
    return key;
  }

  static Schema optionalSchemaOrElse(Schema schema, @NonNull Supplier<Schema> alternative) {
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
  static SchemaBuilder toBuilder(@NonNull Schema schema) {
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
   * @param headers the message headers (can be null)
   * @return a struct schema for the headers
   */
  static Schema forHeaders(Headers headers) {
    final SchemaBuilder builder = SchemaBuilder.struct().optional();
    if (headers != null) {
      final Map<String, List<Schema>> schemas = new HashMap<>();
      StreamSupport.stream(headers.spliterator(), false)
          .forEach(
              header ->
                  schemas
                      .computeIfAbsent(header.key(), k -> new ArrayList<>())
                      .add(header.schema()));
      schemas.forEach((key, value) -> builder.field(key, Merger.mergeHeaderSchemas(value)));
    }
    return builder.build();
  }
}
