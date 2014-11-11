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

import com.beust.jcommander.internal.Lists;
import java.util.Collection;
import java.util.UUID;
import kafka.utils.ZkUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.event.StandardEvent;

public class KafkaDatasetTest extends BaseKafkaTest {

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

  @Test
  public void testDatasetLifecycle() {

    Dataset<StandardEvent> dataset = Datasets.create("dataset:kafka:"
        + zkConnection + "/" + namespace + "/" + name + "?schemaRepo="
        + schemaRepoUrl, new DatasetDescriptor.Builder()
        .schema(StandardEvent.getClassSchema())
        .build(), StandardEvent.class);

    DatasetWriter<StandardEvent> writer = dataset.newWriter();
    Collection<StandardEvent> events = events();
    for (StandardEvent event : events) {
      writer.write(event);
    }
    writer.close();

    Collection<StandardEvent> readEvents = Lists.newArrayList();
    DatasetReader<StandardEvent> reader = dataset.newReader();
    int nEvents = 0;
    while (nEvents < 10) {
      StandardEvent event = reader.next();
      System.out.println(event);
      readEvents.add(event);
      nEvents++;
    }
    reader.close();

    Assert.assertEquals("Read events not equal to written events", events, readEvents);
  }

  private Collection<StandardEvent> events() {
    Collection<StandardEvent> events = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      events.add(StandardEvent.newBuilder()
          .setEventName(String.valueOf(i))
          .setEventInitiator("user")
          .setIp("127.0.0.1")
          .setSessionId(UUID.randomUUID().toString())
          .setTimestamp(System.currentTimeMillis())
          .setUserId(i)
          .build());
    }

    return events;
  }
}
