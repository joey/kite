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

import java.util.UUID;
import org.I0Itec.zkclient.ZkClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.kitesdk.minicluster.KafkaService;
import org.kitesdk.minicluster.MiniCluster;
import org.kitesdk.minicluster.SchemaRepoService;
import org.kitesdk.minicluster.ZookeeperService;
import org.schemarepo.Repository;
import org.schemarepo.client.RESTRepositoryClient;

public class BaseKafkaTest {

  protected static String zkConnection = null;
  protected static String schemaRepoUrl = null;

  protected static MiniCluster miniCluster;
  protected static ZkClient zkClient;
  protected static Repository schemaRepo;

  @BeforeClass
  public static void startMiniCluster() throws Exception {
    String workDir = "target/test/" + KafkaDatasetRepositoryTest.class + "/" + UUID.randomUUID();
    int zkPort = KafkaTestUtils.getAvailablePort();
    int schemaRepoPort = KafkaTestUtils.getAvailablePort();
    zkConnection = "localhost:" + zkPort;
    schemaRepoUrl = "http://localhost:" + schemaRepoPort + "/schema-repo";
    miniCluster = new MiniCluster.Builder()
        .workDir(workDir)
        .zkPort(zkPort)
        .schemaRepoPort(schemaRepoPort)
        .bindIP("127.0.0.1")
        .clean(true)
        .addService(SchemaRepoService.class)
        .addService(ZookeeperService.class)
        .addService(KafkaService.class)
        .build();

    miniCluster.start();

    zkClient = new ZkClient(zkConnection);
    zkClient.setZkSerializer(new DefaultZkSerializer());

    schemaRepo = new RESTRepositoryClient(schemaRepoUrl);
  }

  @AfterClass
  public static void stopMiniCluster() throws Exception {
    zkClient.close();
    miniCluster.stop();

    miniCluster = null;
    zkClient = null;
    schemaRepo = null;
  }

}
