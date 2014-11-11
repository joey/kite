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

import com.google.common.collect.Lists;
import java.util.List;
import kafka.admin.AdminUtils;
import kafka.cluster.Broker;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import scala.collection.Iterator;


class KafkaUtil {

  static final String TOPIC_NAME_SEPERATOR = ".";

  static String topicName(String namespace, String name) {
    return namespace + TOPIC_NAME_SEPERATOR + name;
  }

  static <E> String topicName(Dataset<E> dataset) {
    return topicName(dataset.getNamespace(), dataset.getName());
  }

  static int numPartitions(DatasetDescriptor descriptor) {
    if (descriptor.isPartitioned()) {
      return descriptor.getPartitionStrategy().getCardinality();
    } else {
      return 1;
    }
  }
  static boolean topicExists(ZkClient zkClient, String namespace, String name) {
    return topicExists(zkClient, topicName(namespace, name));
  }
  static boolean topicExists(ZkClient zkClient, String topic) {
    return AdminUtils.topicExists(zkClient, topic) &&
        !ZkUtils.pathExists(zkClient, ZkUtils.getDeleteTopicPath(topic));
  }

  static void deleteTopic(ZkClient zkClient, String namespace, String name) {
    deleteTopic(zkClient, topicName(namespace, name));
  }
  
  static void deleteTopic(ZkClient zkClient, String topic) {
    AdminUtils.deleteTopic(zkClient, topic);

    while (ZkUtils.pathExists(zkClient, ZkUtils.getDeleteTopicPath(topic))) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }
  }

  static String getBrokerListString(ZkClient zkClient) {
    StringBuilder result = new StringBuilder();

    Iterator<Broker> iterator = ZkUtils.getAllBrokersInCluster(zkClient).iterator();
    Broker broker = iterator.next();
    result.append(broker.connectionString());
    while (iterator.hasNext()) {
      result.append(",");
      broker = iterator.next();
      result.append(broker.connectionString());
    }

    return result.toString();
  }

  static List<String> getBrokerList(ZkClient zkClient) {
    List<String> result = Lists.newArrayList();

    Iterator<Broker> iterator = ZkUtils.getAllBrokersInCluster(zkClient).iterator();
    while (iterator.hasNext()) {
      result.add(iterator.next().connectionString());
    }

    return result;
  }


}
