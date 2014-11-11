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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.spi.AbstractDatasetReader;
import org.kitesdk.data.spi.DataModelUtil;
import org.kitesdk.data.spi.ReaderWriterState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.collection.Seq;

public class KafkaAvroReader<E> extends AbstractDatasetReader<E> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaAvroReader.class);
  private static final DecoderFactory DECODER_FACTORY = DecoderFactory.get();

  private final KafkaView<E> view;
  private final String zkConnection;
  private final DatumReader<E> reader;
  private final String topic;
  private final ConcurrentLinkedQueue<byte[]> entityQueue;
  
  private ReaderWriterState state;
  private BinaryDecoder decoder;
  private List<KafkaConsumerTask> tasks;
  private List<Thread> threads;

  KafkaAvroReader(KafkaView<E> view) {
    this.view = view;
    this.zkConnection = this.view.getZkConnection();
    reader = DataModelUtil.getDatumReaderForType(view.getType(),
        view.getDataset().getDescriptor().getSchema());
    topic = KafkaUtil.topicName(this.view.getDataset());
    entityQueue = new ConcurrentLinkedQueue<byte[]>();
    state = ReaderWriterState.NEW;
  }

  @Override
  public boolean hasNext() {
    return !entityQueue.isEmpty();
  }

  @Override
  public E next() {
    byte[] value = entityQueue.poll();
    while (value == null) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
      value = entityQueue.poll();
    }

    try {
      decoder = DECODER_FACTORY.binaryDecoder(value, decoder);
      return reader.read(null, decoder);
    } catch (IOException ex) {
      throw new DatasetIOException("Error decoding entity", ex);
    }
  }

  @Override
  public void close() {
    for (KafkaConsumerTask task : tasks) {
      task.cancel();
    }
    
    for (Thread thread : threads) {
      try {
        thread.join();
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }
    
    state = ReaderWriterState.CLOSED;
  }

  @Override
  public boolean isOpen() {
    return state == ReaderWriterState.OPEN;
  }

  @Override
  public void initialize() {
    ZkClient zkClient = new ZkClient(zkConnection);
    zkClient.setZkSerializer(new DefaultZkSerializer());

    Seq<String> topics = JavaConversions.asScalaBuffer(
        ImmutableList.of(topic)).toList();
    scala.collection.mutable.Map<String, Seq<Object>> partitionsForTopics =
        ZkUtils.getPartitionsForTopics(zkClient, topics);

    int nPartitions = partitionsForTopics.get(topic).get().size();
    tasks = Lists.newArrayListWithCapacity(nPartitions);
    threads = Lists.newArrayListWithCapacity(nPartitions);
    for (int partition = 0; partition < nPartitions; partition++) {
      KafkaConsumerTask task = new KafkaConsumerTask(topic, partition, "kite",
          zkConnection, entityQueue);
      tasks.add(task);
      Thread thread = new Thread(task, "Consumer [" + topic + ", " + partition + "]");
      thread.start();

      threads.add(thread);
    }

    state = ReaderWriterState.OPEN;
  }

}
