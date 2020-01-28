package com.github.f0xdx;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

public class Wrap<R extends ConnectRecord<R>> implements Transformation<R> {

  @Override
  public R apply(R record) {
    // TODO implement applying this transformation to a record
    return null;
  }

  @Override
  public ConfigDef config() {
    return null;
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> configs) {

  }

  // TODO implement this scaffold
}
