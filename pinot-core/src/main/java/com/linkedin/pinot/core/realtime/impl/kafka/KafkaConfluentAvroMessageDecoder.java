/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.realtime.impl.kafka;


import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.serializers.KafkaAvroDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.avro.generic.GenericData.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;

public class KafkaConfluentAvroMessageDecoder implements KafkaMessageDecoder {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAvroMessageDecoder.class);
  private KafkaAvroDecoder decoder;
  private AvroRecordToPinotRowGenerator avroRecordConvetrer;

  @Override
  public void init(Map<String, String> props, Schema indexingSchema, String topicName) throws Exception {

    for (String key : props.keySet()) {
      System.out.println(key + ":" + props.get(key));
    }

    Properties properties = new Properties();
    properties.putAll(props);
    VerifiableProperties vProps = new VerifiableProperties(properties);
    this.decoder = new KafkaAvroDecoder(vProps);
    this.avroRecordConvetrer = new AvroRecordToPinotRowGenerator(indexingSchema);
  }

  @Override
  public GenericRow decode(byte[] payload) {

    if (payload == null || payload.length == 0) {
      return null;
    }

    try {
      Record avroRecord = (Record) decoder.fromBytes(payload);
      return avroRecordConvetrer.transform(avroRecord, avroRecord.getSchema());
    } catch (Exception e) {
      LOGGER.error("Caught exception while reading message", e);
      return null;
    }
  }
}
