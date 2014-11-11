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

import java.net.URI;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.Pair;
import org.kitesdk.data.spi.Registration;

public class LoaderTest extends BaseKafkaTest {
  
  @Test
  public void testRepoUri() {
    Pair<DatasetRepository, Map<String, String>> repoOptions =
        Registration.lookupRepoUri(URI.create("kafka:" + zkConnection
            + "?schemaRepo=" + schemaRepoUrl));

    Assert.assertNotNull("Null DatasetRepository", repoOptions.first());
    Assert.assertNotNull("Null options", repoOptions.second());
    Assert.assertEquals("Zookeeper option doesn't match the URI", zkConnection, 
        repoOptions.second().get("zk"));
    Assert.assertEquals("Zookeeper option doesn't match the URI", schemaRepoUrl, 
        repoOptions.second().get("schemaRepo"));
  }
  
  @Test
  public void testDatasetUriWithoutNamespace() {
    Pair<DatasetRepository, Map<String, String>> repoOptions =
        Registration.lookupDatasetUri(URI.create("kafka:" + zkConnection 
            + "/events" + "?schemaRepo=" + schemaRepoUrl));

    Assert.assertNotNull("Null DatasetRepository", repoOptions.first());
    Assert.assertNotNull("Null options", repoOptions.second());
    Assert.assertEquals("Zookeeper option doesn't match the URI", zkConnection,
        repoOptions.second().get("zk"));
    Assert.assertEquals("Zookeeper option doesn't match the URI", schemaRepoUrl, 
        repoOptions.second().get("schemaRepo"));
    Assert.assertEquals("Dataset namespace doesn't default", "default",
        repoOptions.second().get("namespace"));
    Assert.assertEquals("Dataset name doesn't match the URI", "events",
        repoOptions.second().get("dataset"));
  }

  @Test
  public void testDatasetUriWithNamespace() {
    Pair<DatasetRepository, Map<String, String>> repoOptions =
        Registration.lookupDatasetUri(URI.create("kafka:" + zkConnection 
            + "/ns/events" + "?schemaRepo=" + schemaRepoUrl));

    Assert.assertNotNull("Null DatasetRepository", repoOptions.first());
    Assert.assertNotNull("Null options", repoOptions.second());
    Assert.assertEquals("Zookeeper option doesn't match the URI", zkConnection,
        repoOptions.second().get("zk"));
    Assert.assertEquals("Zookeeper option doesn't match the URI", schemaRepoUrl, 
        repoOptions.second().get("schemaRepo"));
    Assert.assertEquals("Namespace doesn't match the URI", "ns",
        repoOptions.second().get("namespace"));
    Assert.assertEquals("Dataset name doesn't match the URI", "events",
        repoOptions.second().get("dataset"));
  }
  
}
