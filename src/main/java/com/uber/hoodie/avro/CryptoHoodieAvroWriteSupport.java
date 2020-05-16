/*
 * Copyright (c) 2018 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.avro;

import com.uber.hoodie.common.BloomFilter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.ExtType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.codehaus.jackson.JsonNode;

public class CryptoHoodieAvroWriteSupport extends HoodieAvroWriteSupport {

  public CryptoHoodieAvroWriteSupport(
      MessageType schema, Schema avroSchema, BloomFilter bloomFilter) {
    super(schema, avroSchema, bloomFilter);
  }

  // TODO: Nest columns will be added later
  @Override
  public WriteContext init(Configuration configuration) {
    WriteContext writeContext = super.init(configuration);
    MessageType messageType = writeContext.getSchema();
    List<Type> newFields = new ArrayList<>();
    for (Type field : messageType.getFields()) {
      ExtType<Object> cryptoField = new ExtType<>(field);

      Schema avroSchema = super.getAvroSchema();
      Map<String, Object> metadata = getMetadata(avroSchema, field);

      cryptoField.setMetadata(metadata);
      newFields.add(cryptoField);
    }

    MessageType newMessageType = new MessageType(messageType.getName(), newFields);
    Map<String, String> extraMetadata = new HashMap<>();
    return new WriteContext(newMessageType, extraMetadata);
  }

  private Map<String, Object> getMetadata(Schema avroSchema, Type field) {
    Map<String, Object> newProps = new HashMap<>();
    for (Schema.Field avroField : avroSchema.getFields()) {
      if (field.getName().equals(avroField.name())) {
        Map<String, JsonNode> props = avroField.getJsonProps();
        for (String key : props.keySet()) {
          newProps.put(key, props.get(key));
        }
        return newProps;
      }
    }
    return newProps;
  }
}
