/*
 * Copyright 2014 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data.kafka;

import com.google.common.io.Closeables;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.message.SnappyCompressionCodec;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.DefaultEncoder;
import org.I0Itec.zkclient.ZkClient;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.spi.AbstractDatasetWriter;
import org.kitesdk.data.spi.DataModelUtil;
import org.kitesdk.data.spi.EntityAccessor;
import org.kitesdk.data.spi.PartitionKey;
import org.kitesdk.data.spi.ReaderWriterState;


public class KafkaAvroWriter<E> extends AbstractDatasetWriter<E> {

  private static final EncoderFactory ENCODER_FACTORY = EncoderFactory.get();
  private final KafkaView<E> view;
  private final String zkConnection;
  private final DatumWriter<E> writer;
  private final EntityAccessor<E> accessor;
  private final String topic;
  private ReaderWriterState state;
  private BinaryEncoder encoder;
  private Producer<PartitionKey, byte[]> producer;

  KafkaAvroWriter(KafkaView<E> view) {
    this.view = view;
    this.zkConnection = this.view.getZkConnection();
    writer = DataModelUtil.getDatumWriterForType(view.getType(),
        view.getDataset().getDescriptor().getSchema());
    accessor = view.getAccessor();
    topic = KafkaUtil.topicName(this.view.getDataset());
    state = ReaderWriterState.NEW;
  }

  @Override
  public void write(E entity) {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    encoder = ENCODER_FACTORY.binaryEncoder(stream, encoder);

    try {
      writer.write(entity, encoder);
      encoder.flush();
    } catch (IOException ex) {
      throw new DatasetIOException("Failed to serialize " + entity, ex);
    } finally {
      Closeables.closeQuietly(stream);
    }

    PartitionKey key = null;
    if (view.getDataset().getDescriptor().isPartitioned()) {
      PartitionKey.partitionKeyForEntity(
          view.getDataset().getDescriptor().getPartitionStrategy(), entity,
          accessor);
    }
    producer.send(new KeyedMessage<PartitionKey, byte[]>(topic, key, stream.toByteArray()));
  }

  @Override
  public void flush() {
  }

  @Override
  public void sync() {
  }

  @Override
  public void close() {
    producer.close();
    state = ReaderWriterState.CLOSED;
  }

  @Override
  public boolean isOpen() {
    return state.equals(ReaderWriterState.OPEN);
  }

  @Override
  public void initialize() {
    ZkClient zkClient = new ZkClient(zkConnection);
    zkClient.setZkSerializer(new DefaultZkSerializer());

    Properties properties = new Properties();

    properties.setProperty("zk.connect", zkConnection);
    properties.setProperty("metadata.broker.list", KafkaUtil.getBrokerListString(zkClient));
    properties.setProperty("serializer.class", DefaultEncoder.class.getName());
    properties.setProperty("partitioner.class", KafkaPartitioner.class.getName());
    
    // TODO: Set compression codec based on dataset config
    properties.setProperty("compression.codec", SnappyCompressionCodec.name());
    properties.setProperty("message.send.max.retries", "10");
    properties.setProperty("batch.num.messages", "1000");

    producer = new Producer<PartitionKey, byte[]>(new ProducerConfig(properties));

    state = ReaderWriterState.OPEN;
  }
}
