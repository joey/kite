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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.net.URI;
import java.util.Collection;
import java.util.Properties;
import kafka.admin.AdminUtils;
import kafka.log.LogConfig;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.spi.AbstractDatasetRepository;
import org.kitesdk.data.spi.Compatibility;
import org.kitesdk.data.spi.MetadataProvider;
import org.schemarepo.CacheRepository;
import org.schemarepo.InMemoryCache;
import org.schemarepo.Repository;
import org.schemarepo.client.RESTRepositoryClient;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.collection.Set;


public class KafkaDatasetRepository extends AbstractDatasetRepository {

  private final String zkConnection;
  private final ZkClient zkClient;
  private final URI repositoryUri;
  private final KafkaMetadataProvider metadataProvider;

  KafkaDatasetRepository(final String zkConnection, final String schemaRepoUrl,
      final URI repositoryUri) {
    this.zkConnection = zkConnection;
    this.zkClient = new ZkClient(this.zkConnection);
    this.zkClient.setZkSerializer(new DefaultZkSerializer());
    this.repositoryUri = repositoryUri;

    Repository repo = new CacheRepository(
        new RESTRepositoryClient(schemaRepoUrl), new InMemoryCache());
    this.metadataProvider = new KafkaMetadataProvider(repo, zkClient);
  }

  @Override
  public <E> Dataset<E> load(String namespace, String name, Class<E> type) {
    Preconditions.checkArgument(KafkaUtil.topicExists(zkClient, namespace, name),
        "Dataset %s in namespace %s doesn't exist",
        name, namespace);

    DatasetDescriptor descriptor = metadataProvider.load(namespace, name);

    return new KafkaAvroDataset<E>(zkConnection, namespace, name, type,
        descriptor.getSchema(), descriptor, repositoryUri);
  }

  @Override
  public <E> Dataset<E> create(String namespace, String name, final DatasetDescriptor descriptor, Class<E> type) {
    Compatibility.checkDatasetName(namespace, name);
    Compatibility.checkDescriptor(descriptor);

    String topic = KafkaUtil.topicName(namespace, name);
    Preconditions.checkArgument(!KafkaUtil.topicExists(zkClient, topic),
        "Dataset %s in namespace %s already exists", name, namespace);

    int numPartitions = KafkaUtil.numPartitions(descriptor);
    int replicationFactor = ZkUtils.getAllBrokersInCluster(zkClient).length();
    if (descriptor.hasProperty(KafkaConfig.KAFKA_REPLICATION_FACTOR)) {
     replicationFactor = Integer.parseInt(
         descriptor.getProperty(KafkaConfig.KAFKA_REPLICATION_FACTOR));
    }

    DatasetDescriptor createdDescriptor = metadataProvider.create(namespace, name, descriptor);

    AdminUtils.createTopic(zkClient, topic, numPartitions, replicationFactor,
        propertiesFromDescriptor(createdDescriptor));

    metadataProvider.createOrUpdateSchemaId(namespace, name, createdDescriptor);

    return new KafkaAvroDataset<E>(zkConnection, namespace, name, type,
        descriptor.getSchema(), descriptor, repositoryUri);
  }

  @Override
  public <E> Dataset<E> update(String namespace, String name, DatasetDescriptor descriptor, Class<E> type) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public boolean delete(String namespace, String name) {
    if (!exists(namespace, name)) {
      return false;
    }

    metadataProvider.delete(namespace, name);
    KafkaUtil.deleteTopic(zkClient, namespace, name);

    return true;
  }

  @Override
  public boolean exists(String namespace, String name) {
    return metadataProvider.exists(namespace, name) &&
        KafkaUtil.topicExists(zkClient, namespace, name);
  }

  @Override
  public Collection<String> namespaces() {
    Collection<String> namespaces = Lists.newArrayList();
    Seq<String> allTopics = ZkUtils.getAllTopics(zkClient);

    Iterator<String> topicIterator = allTopics.iterator();
    while(topicIterator.hasNext()) {
      String topic = topicIterator.next();
      if (topic.contains(KafkaUtil.TOPIC_NAME_SEPERATOR)) {
        namespaces.add(topic.substring(0,
            topic.indexOf(KafkaUtil.TOPIC_NAME_SEPERATOR)));
      }
    }

    return namespaces;
  }

  @Override
  public Collection<String> datasets(String namespace) {
    Collection<String> datasets = Lists.newArrayList();
    Seq<String> allTopics = ZkUtils.getAllTopics(zkClient);

    String prefix = KafkaUtil.topicName(namespace, "");

    Iterator<String> topicIterator = allTopics.iterator();
    while(topicIterator.hasNext()) {
      String topic = topicIterator.next();
      if (topic.startsWith(prefix)) {
        String name = topic.substring(
            topic.indexOf(KafkaUtil.TOPIC_NAME_SEPERATOR) + 1);
        if (exists(namespace, name)) {
          datasets.add(name);
        }
      }
    }

    return datasets;
  }

  @Override
  public URI getUri() {
    return repositoryUri;
  }

  private Properties propertiesFromDescriptor(DatasetDescriptor descriptor) {
    Set<String> kafkaConfigNames = LogConfig.ConfigNames();
    Properties properties = new Properties();
    for (String property : descriptor.listProperties()) {
      if (kafkaConfigNames.contains(property)) {
        properties.put(property, descriptor.getProperty(property));
      }
    }

    return properties;
  }
}
