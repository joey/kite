/*
 * Copyright 2014 Cloudera.
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
package org.kitesdk.data.remote;

import java.net.InetSocketAddress;
import org.apache.avro.Schema;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.reflect.ReflectRequestor;
import org.apache.avro.reflect.ReflectData;
import static org.kitesdk.data.service.RemoteDatasetTestUtilities.*;

import org.junit.*;
import static org.junit.Assert.*;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.remote.protocol.RemoteDataProtocol;
import org.kitesdk.data.remote.service.DatasetServer;
import org.kitesdk.data.remote.service.ServiceReflectData;
import org.kitesdk.data.service.MemoryDataset;
import org.kitesdk.data.service.User;

public class RemoteDatasetWriterTest {
  private RemoteDataset<User> dataset;
  private NettyServer server;
  
  public RemoteDatasetWriterTest() {
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
  }
  
  @Before
  public void setUp() throws Exception {
    MemoryDataset<User> memoryDataset = createEmptyMemoryDataset();
    server = DatasetServer.startServer(memoryDataset, 0);

    Schema schema = new ReflectData().getSchema(User.class);
    NettyTransceiver client = new NettyTransceiver(new InetSocketAddress(server.getPort()));
    @SuppressWarnings("unchecked")
    RemoteDataProtocol<User> proxy = ReflectRequestor.getClient(
        RemoteDataProtocol.class, client,
        new ServiceReflectData(RemoteDataProtocol.class, schema));
    dataset = new RemoteDataset<User>(proxy, proxy.getRootHandle(), schema, User.class);
  }
  
  @After
  public void tearDown() {
    server.close();
  }

  @Test
  public void testIsOpen() {
    DatasetWriter<User> instance = dataset.newWriter(); 
    boolean expResult = true;
    boolean result = instance.isOpen();
    assertEquals(expResult, result);
  }
}
