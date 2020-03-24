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
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/** Helper class for {@link Schema} related tasks. */
public class Schemas {

  /** Wrapper class for unique combinations of schemas. */
  @Value(staticConstructor = "of")
  public static class KeyValueSchema {
    Schema key;
    Schema value;
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
  public static <R extends ConnectRecord<R>> KeyValueSchema schemaOf(@NonNull R record) {
    return KeyValueSchema.of(record.keySchema(), record.valueSchema());
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
      builder = builder.version(schema.version()).doc(schema.doc());

      if (schema.defaultValue() != null) {
        builder = builder.defaultValue(schema.defaultValue());
      }

      if (schema.isOptional()) {
        return builder.optional();
      }
    }

    return builder;
  }
}
