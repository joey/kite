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
package org.kitesdk.minicluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.Test;
import static org.junit.Assert.*;

public class SchemaRepoServiceTest {
  
  @Test
  public void testStartStop() throws IOException, InterruptedException {
    String workDir = "target/test/" + SchemaRepoServiceTest.class +
        "/" + UUID.randomUUID();

    Service.ServiceConfig serviceConfig = new Service.ServiceConfig();
    serviceConfig.set(MiniCluster.WORK_DIR_KEY, workDir);
    serviceConfig.set(MiniCluster.BIND_IP_KEY, "127.0.0.1");
    serviceConfig.set(MiniCluster.SCHEMA_REPO_PORT_KEY, "2876");
    SchemaRepoService service = new SchemaRepoService();
    service.configure(serviceConfig);
    service.start();
    service.stop();
  }

  @Test
  public void testDependencies() {
    SchemaRepoService service = new SchemaRepoService();
    List<Class<? extends Service>> expResult = new ArrayList<Class<? extends Service>>();
    List<Class<? extends Service>> result = service.dependencies();
    assertEquals(expResult, result);
  }
  
}
