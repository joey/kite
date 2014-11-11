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
import com.google.common.collect.Maps;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.I0Itec.zkclient.ZkClient;
import org.kitesdk.data.DatasetException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerTask implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerTask.class);

  private final String topic;
  private final int partition;
  private final String clientName;
  private final String zkConnection;
  private final ConcurrentLinkedQueue<byte[]> queue;
  private SimpleConsumer consumer;
  private List<String> replicaBrokers;
  private String leadBroker;
  private int port;
  private int numErrors;
  private boolean canceled;

  KafkaConsumerTask(String topic, int partition, String clientName,
      String zkConnection, ConcurrentLinkedQueue<byte[]> queue) {
    this.topic = topic;
    this.partition = partition;
    this.clientName = clientName;
    this.zkConnection = zkConnection;
    this.queue = queue;

    numErrors = 0;
    canceled = false;
  }

  @Override
  public void run() {
    ZkClient zkClient = new ZkClient(zkConnection);
    zkClient.setZkSerializer(new DefaultZkSerializer());

    replicaBrokers = KafkaUtil.getBrokerList(zkClient);

    PartitionMetadata metadata = findLeader();
    if (metadata == null || metadata.leader() == null) {
      throw new DatasetException("Can't find leader for [" + topic + ", "
          + partition + "] ");
    }

    leadBroker = metadata.leader().host();
    port = metadata.leader().port();

    consumer = newConsumer();
    long readOffset = getLastOffset(consumer, topic, partition,
        OffsetRequest.EarliestTime(), clientName);

    while (!canceled) {
      if (consumer == null) {
        consumer = newConsumer();
      }

      FetchRequest request = new FetchRequestBuilder()
          .clientId(clientName)
          .addFetch(topic, partition, readOffset, 100000)
          .build();
      FetchResponse response = consumer.fetch(request);

      if (response.hasError()) {
        numErrors++;
        short code = response.errorCode(topic, partition);
        LOG.warn("Error fetching data from broker " + leadBroker + " Reason: "
            + code);

        if (code == ErrorMapping.OffsetOutOfRangeCode()) {
          readOffset = getLastOffset(consumer, topic, partition,
              OffsetRequest.LatestTime(), clientName);
          continue;
        }
        consumer.close();
        consumer = null;
        leadBroker = findNewLeader(leadBroker);
        continue;
      }

      long numRead = 0;
      for (MessageAndOffset messageAndOffset : response.messageSet(topic, partition)) {
        long currentOffset = messageAndOffset.offset();
        if (currentOffset < readOffset) {
          LOG.warn("Found an old offset: " + currentOffset + " Expecting: "
              + readOffset);
          continue;
        }

        readOffset = messageAndOffset.nextOffset();
        ByteBuffer payload = messageAndOffset.message().payload();

        byte[] value = new byte[payload.limit()];
        payload.get(value);
        queue.add(value);
        numRead++;
      }

      if (numRead == 0) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private SimpleConsumer newConsumer() {
    return new SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName);
  }

  public void cancel() {
    canceled = true;
  }

  public static long getLastOffset(SimpleConsumer consumer, String topic, int partition,
      long whichTime, String clientName) {
    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = Maps.newHashMap();
    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
    kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
        requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
    OffsetResponse response = consumer.getOffsetsBefore(request);

    if (response.hasError()) {
      LOG.error("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
      return 0;
    }
    long[] offsets = response.offsets(topic, partition);
    return offsets[0];
  }

  private String findNewLeader(String oldLeader) throws DatasetException {
    for (int i = 0; i < 3; i++) {
      PartitionMetadata metadata = findLeader();
      if (metadata == null) {
        // Try again
      } else if (metadata.leader() == null) {
        // Try again
      } else if (oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
        // first time through if the leader hasn't changed give ZooKeeper a second to recover
        // second time, assume the broker did recover before failover, or it was a non-Broker issue
        //
      } else {
        return metadata.leader().host();
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    }
    LOG.error("Unable to find new leader after Broker failure.");
    throw new DatasetException("Unable to find new leader after Broker failure.");
  }

  private PartitionMetadata findLeader() {
    PartitionMetadata returnMetaData = null;
    loop:
    for (String broker : replicaBrokers) {
      SimpleConsumer leaderLookupConsumer = null;
      try {
        int colonIdx = broker.indexOf(":");
        if (colonIdx == -1) {
          LOG.warn("Invalid broker connection string " + broker);
          continue;
        }
        String brokerHost = broker.substring(0, colonIdx);
        int brokerPort = Integer.parseInt(broker.substring(colonIdx+1));
        leaderLookupConsumer = new SimpleConsumer(brokerHost, brokerPort, 100000, 64 * 1024, "leaderLookup");
        TopicMetadataRequest req = new TopicMetadataRequest(ImmutableList.of(topic));
        kafka.javaapi.TopicMetadataResponse resp = leaderLookupConsumer.send(req);

        List<TopicMetadata> metaData = resp.topicsMetadata();
        for (TopicMetadata item : metaData) {
          for (PartitionMetadata part : item.partitionsMetadata()) {
            if (part.partitionId() == partition) {
              returnMetaData = part;
              break loop;
            }
          }
        }
      } catch (Exception e) {
        LOG.warn("Error communicating with Broker [" + broker + "] to find Leader for [" + topic
            + ", " + partition + "] Reason: " + e.getMessage());
        LOG.debug("Stack trace follows", e);
      } finally {
        if (leaderLookupConsumer != null) {
          leaderLookupConsumer.close();
        }
      }
    }

    if (returnMetaData != null) {
      replicaBrokers.clear();
      for (Broker replica : returnMetaData.replicas()) {
        replicaBrokers.add(replica.connectionString());
      }
    }
    return returnMetaData;
  }

}
