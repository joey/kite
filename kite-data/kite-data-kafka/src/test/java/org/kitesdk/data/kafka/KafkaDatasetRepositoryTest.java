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

import com.google.common.collect.Sets;
import java.net.URI;
import java.util.Collection;
import kafka.utils.ZkUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.event.StandardEvent;

public class KafkaDatasetRepositoryTest extends BaseKafkaTest {

  private static final String namespace = "default";
  private static final String name = "events";
  private static final String topic = KafkaUtil.topicName(namespace, name);

  @Before
  @After
  public void deleteTopic() throws Exception {
    String schemaIdPath = ZkUtils.getTopicPath(KafkaUtil.topicName(namespace, name))
        + "/" + KafkaConfig.KITE_SCHEMA_ID;
    if (ZkUtils.pathExists(zkClient, schemaIdPath)) {
      ZkUtils.deletePath(zkClient, schemaIdPath);
    }

    if (KafkaUtil.topicExists(zkClient, topic)) {
      KafkaUtil.deleteTopic(zkClient, topic);
    }
  }

  private boolean subjectExists(String topic) {
    return schemaRepo.lookup(topic) != null;
  }
  
  @Test
  public void testDatasetRepositoryLifecycle() {
    assertTrue("Kafka topic already exists", !KafkaUtil.topicExists(
        zkClient, topic));

    KafkaDatasetRepository repo = new KafkaDatasetRepository(zkConnection,
        schemaRepoUrl, URI.create("repo:kafka:" + zkConnection + "?schemaRepo=" + schemaRepoUrl));
    
    assertFalse("repo.exists() returned true when the dataset doesn't exist",
        repo.exists(namespace, name));

    repo.create(namespace, name, new DatasetDescriptor.Builder()
        .schema(StandardEvent.getClassSchema()).build(), StandardEvent.class);

    assertTrue("Kafka topic not created",
        KafkaUtil.topicExists(zkClient, topic));
    assertTrue("Schema repo subject not created", subjectExists(topic));

    assertTrue("repo.exists() returned false when the dataset exists", repo.exists(namespace, name));

    repo.delete(namespace, name);

    assertTrue("Kafka topic still exists",
        !KafkaUtil.topicExists(zkClient, topic));

    assertFalse("repo.exists() returned true when the dataset doesn't exist",
        repo.exists(namespace, name));
  }

  @Test
  public void testNamespaces() {
    KafkaDatasetRepository repo = new KafkaDatasetRepository(zkConnection,
        schemaRepoUrl, URI.create("repo:kafka:" + zkConnection + "?schemaRepo=" + schemaRepoUrl));

    repo.create(namespace, name, new DatasetDescriptor.Builder()
        .schema(StandardEvent.getClassSchema()).build(), StandardEvent.class);

    Collection<String> expResult = Sets.newHashSet(namespace);
    Collection<String> result = repo.namespaces();
    assertTrue("Unexpected namespaces", expResult.containsAll(result)
        && result.containsAll(expResult));
  }

  @Test
  public void testDatasets() {
    KafkaDatasetRepository repo = new KafkaDatasetRepository(zkConnection,
        schemaRepoUrl, URI.create("repo:kafka:" + zkConnection + "?schemaRepo=" + schemaRepoUrl));

    repo.create(namespace, name, new DatasetDescriptor.Builder()
        .schema(StandardEvent.getClassSchema()).build(), StandardEvent.class);

    Collection<String> expResult = Sets.newHashSet(name);
    Collection<String> result = repo.datasets(namespace);
    assertTrue("Unexpected datasets", expResult.containsAll(result)
        && result.containsAll(expResult));
  }
}
