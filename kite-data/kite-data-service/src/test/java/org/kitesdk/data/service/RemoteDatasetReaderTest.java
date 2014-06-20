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
package org.kitesdk.data.service;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.reflect.ReflectRequestor;
import org.apache.avro.reflect.ReflectData;
import org.junit.*;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.TestDatasetReaders;
import org.kitesdk.data.remote.RemoteDataset;
import org.kitesdk.data.remote.protocol.RemoteDataProtocol;
import org.kitesdk.data.remote.service.DatasetServer;
import org.kitesdk.data.remote.service.ServiceReflectData;
import org.kitesdk.data.spi.filesystem.DatasetTestUtilities.RecordValidator;

public class RemoteDatasetReaderTest extends TestDatasetReaders {

  static final int port = 42424;
  static MemoryDataset<User> dataset;
  static List<User> data = Arrays.asList(
      new User("Joey", "blue"),
      new User("Sean", "green"),
      new User("Alex", "red"),
      new User("Ryan", "orange"),
      new User("Tom", "black"));

  @BeforeClass
  public static void setUpClass() {
    dataset = new MemoryDataset.Builder().name("users").
        descriptor(new DatasetDescriptor.Builder().schema(User.class).build()).
        data(data).
        build();
    DatasetServer.startServer(dataset, port);
  }

  @Override
  public DatasetReader newReader() throws IOException {
    NettyTransceiver client = new NettyTransceiver(new InetSocketAddress(port));
    @SuppressWarnings("unchecked")
    RemoteDataProtocol<User> proxy = ReflectRequestor.getClient(
        RemoteDataProtocol.class, client,
        new ServiceReflectData(RemoteDataProtocol.class,
          new ReflectData().getSchema(User.class)));
    RemoteDataset<User> remoteDataset = new RemoteDataset<User>(proxy,
        proxy.getRootHandle(), User.class);
    return remoteDataset.newReader();
  }

  @Override
  public int getTotalRecords() {
    return 5;
  }

  @Override
  public RecordValidator getValidator() {
    return new MyRecordValidator();
  }

  public class MyRecordValidator implements RecordValidator<User> {

    @Override
    public void validate(User record, int recordNum) {
      if (record == null) {
        throw new NullPointerException("Unexpected null record from Reader");
      }

      Preconditions.checkArgument(record.equals(data.get(recordNum)),
          "Record %s does not equal %s", record, data.get(recordNum));
    }
    
  }

}
