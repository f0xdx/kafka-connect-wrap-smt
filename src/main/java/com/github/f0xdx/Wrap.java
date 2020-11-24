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

import static com.github.f0xdx.Schemas.cacheKey;
import static com.github.f0xdx.Schemas.optionalSchemaOrElse;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA;
import static org.apache.kafka.connect.data.SchemaProjector.project;
import static org.apache.kafka.connect.transforms.util.Requirements.requireSinkRecord;

import com.github.f0xdx.Schemas.SchemaCacheKey;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.*;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

/**
 * Single message transformation (SMT) that wraps key, value and meta-data (partition, offset,
 * timestamp, topic) of existing records of type {@link R} into a new, structured record. This can
 * be used, e.g., to ingest kafka messages for further analysis or debugging into other systems. In
 * contrast to other SMTs, this SMT does not offer a dedicated key and value version. It rather
 * groups key and value into a single structured wrapper, leaving the original key unchanged.
 *
 * <p>Note that this SMT only applies to sink connectors, i.e., connectors that export data from
 * kafka. If used with source connectors, the current implementation will raise a {@link
 * DataException}.
 *
 * @param <R> Type extending {@link ConnectRecord}
 */
@Getter(AccessLevel.PACKAGE)
public class Wrap<R extends ConnectRecord<R>> implements Transformation<R> {

  @SuppressWarnings("unused")
  public static final String OVERVIEW_DOC = "Wraps key, record and metadata into a new record";

  public static final String INCLUDE_HEADERS_CONFIG = "include.headers";

  public static final ConfigDef CONFIG_DEF =
      (new ConfigDef())
          .define(
              INCLUDE_HEADERS_CONFIG,
              ConfigDef.Type.BOOLEAN,
              false,
              ConfigDef.Importance.MEDIUM,
              "flag to toggle inclusion of kafka headers");

  static final String PURPOSE = "wrapping key, value and metadata into record";
  static final String TOPIC = "topic";
  static final String PARTITION = "partition";
  static final String OFFSET = "offset";
  static final String TIMESTAMP = "timestamp";
  static final String TIMESTAMP_TYPE = "timestamp_type";
  static final String KEY = "key";
  static final String VALUE = "value";
  static final String HEADERS = "headers";

  private volatile boolean includeHeaders;
  private Cache<SchemaCacheKey, Schema> schemaUpdateCache;
  private Cache<String, Schema> topicSchemaCache;

  /**
   * Retrieve a {@link Schema} from the cache, update cache on cache miss.
   *
   * @param record the current record of type {@link R}
   * @return the {@link Schema} for wrapped records
   */
  synchronized Schema getSchema(@NonNull R record) {
    val keyValueSchema = cacheKey(record, includeHeaders);
    var schema = schemaUpdateCache.get(keyValueSchema);

    if (schema == null) { // cache miss
      val builder =
          SchemaBuilder.struct()
              .field(TOPIC, Schema.STRING_SCHEMA)
              .field(PARTITION, Schema.INT32_SCHEMA)
              .field(OFFSET, Schema.INT64_SCHEMA)
              .field(TIMESTAMP, Schema.INT64_SCHEMA)
              .field(TIMESTAMP_TYPE, Schema.STRING_SCHEMA)
              .field(
                  KEY,
                  optionalSchemaOrElse(
                      record.keySchema(), () -> this.lastKeySchema(record.topic())))
              .field(
                  VALUE,
                  optionalSchemaOrElse(
                      record.valueSchema(), () -> this.lastValueSchema(record.topic())));

      if (includeHeaders) {
        builder.field(HEADERS, Schemas.forHeaders(record.headers()));
      }

      schema = builder.build();

      schemaUpdateCache.put(keyValueSchema, schema);
      topicSchemaCache.put(record.topic(), schema);
    }

    return schema;
  }

  synchronized Schema lastKeySchema(String topic) {
    return Optional.ofNullable(topicSchemaCache.get(topic))
        .map(schema -> schema.field(KEY).schema())
        .orElse(OPTIONAL_STRING_SCHEMA);
  }

  synchronized Schema lastValueSchema(String topic) {
    return Optional.ofNullable(topicSchemaCache.get(topic))
        .map(schema -> schema.field(VALUE).schema())
        .orElse(OPTIONAL_STRING_SCHEMA);
  }

  /**
   * Handle processing for schema-less {@link R}s.
   *
   * @param record the current record of type {@link R}
   * @return a new record of type {@link R} that wraps Key / Value and Meta-data
   */
  R applyWithoutSchema(@NonNull R record) {

    val sinkRecord = requireSinkRecord(record, PURPOSE);
    val result = new HashMap<String, Object>(8);

    result.put(TOPIC, sinkRecord.topic());
    result.put(PARTITION, sinkRecord.kafkaPartition());
    result.put(OFFSET, sinkRecord.kafkaOffset());
    result.put(TIMESTAMP, sinkRecord.timestamp());
    result.put(TIMESTAMP_TYPE, sinkRecord.timestampType().name);
    result.put(KEY, sinkRecord.key());
    result.put(VALUE, sinkRecord.value());

    if (includeHeaders) {
      result.put(HEADERS, sinkRecord.headers());
    }

    return newRecord(record, null, result);
  }

  /**
   * Handle processing for schema-less {@link R}s.
   *
   * @param record the current record of type {@link R}
   * @return a new record of type {@link R} that wraps Key / Value and Meta-data
   */
  R applyWithSchema(@NonNull R record) {
    val sinkRecord = requireSinkRecord(record, PURPOSE);
    val schema = getSchema(record);
    val result =
        new Struct(schema)
            .put(TOPIC, sinkRecord.topic())
            .put(PARTITION, sinkRecord.kafkaPartition())
            .put(OFFSET, sinkRecord.kafkaOffset())
            .put(TIMESTAMP, sinkRecord.timestamp())
            .put(TIMESTAMP_TYPE, sinkRecord.timestampType().name);

    if (sinkRecord.keySchema() != null) {
      result.put(
          KEY, project(sinkRecord.keySchema(), sinkRecord.key(), schema.field(KEY).schema()));
    } else {
      assert record.key() == null : "null key with null schema cannot be wrapped with schema";
      result.put(KEY, null);
    }

    if (sinkRecord.valueSchema() != null) {
      result.put(
          VALUE,
          project(sinkRecord.valueSchema(), sinkRecord.value(), schema.field(VALUE).schema()));
    } else {
      assert record.value() == null : "null value with null schema cannot be wrapped with schema";
      result.put(VALUE, null);
    }

    if (includeHeaders) {
      result.put(HEADERS, sinkRecord.headers());
    }

    return newRecord(record, schema, result);
  }

  /**
   * Process the current record of type {@link R}. Method specified in {@link Transformation}.
   *
   * @param record the current record of type {@link R}
   * @return a new record of type {@link R} that wraps Key / Value and Meta-data
   */
  @Override
  public R apply(R record) {

    if (record != null) {
      if ((record.keySchema() != null || record.key() == null)
          && (record.valueSchema() != null || record.value() == null)) {
        return applyWithSchema(record);
      }
      return applyWithoutSchema(record);
    }
    return null;
  }

  /**
   * Provides configuration information. Method specified in {@link Transformation}.
   *
   * @return the {@link ConfigDef} of this SMT
   */
  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  /** Close the processing and clear the cache. Method specified in {@link Transformation}. */
  @Override
  public void close() {
    schemaUpdateCache = null;
    topicSchemaCache = null;
  }

  /**
   * Configure this SMT. Method specified in {@link Transformation}.
   *
   * @param configs a {@link Map} containing the requested configuration parameters
   */
  @Override
  public void configure(Map<String, ?> configs) {
    val config = new SimpleConfig(CONFIG_DEF, configs);
    includeHeaders = config.getBoolean(INCLUDE_HEADERS_CONFIG);

    schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    topicSchemaCache = new SynchronizedCache<>(new LRUCache<>(16));
  }

  /**
   * Static factory method for new records of type {@link R}.
   *
   * @param record the current record of type {@link R}
   * @param schema the schema for the new record, or <code>null</code>
   * @param value the value {@link Object} for the new record
   * @param <R> the type {@link R} for the new record
   * @return the new record of type {@link R}
   */
  static <R extends ConnectRecord<R>> R newRecord(
      @NonNull R record, Schema schema, @NonNull Object value) {
    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        schema,
        value,
        record.timestamp());
  }
}
