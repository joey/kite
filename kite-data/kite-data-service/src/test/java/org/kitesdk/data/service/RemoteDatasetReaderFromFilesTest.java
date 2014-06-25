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

import static org.kitesdk.data.service.RemoteDatasetTestUtilities.*;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.ipc.NettyServer;
import org.junit.*;
import org.kitesdk.data.*;
import org.kitesdk.data.remote.RemoteDatasetRepository;
import org.kitesdk.data.remote.service.DatasetRepositoryServer;
import org.kitesdk.data.spi.filesystem.DatasetTestUtilities.RecordValidator;

public class RemoteDatasetReaderFromFilesTest extends TestDatasetReaders {

  static NettyServer server;
  static final int port = 42424;
  static DatasetRepository repository;

  @BeforeClass
  public static void setUpClass() throws IOException {
    server = DatasetRepositoryServer.startServer("repo:file:///tmp/data", port);

    repository = new RemoteDatasetRepository("localhost", port);
    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(User.class)
        .build();
    if (repository.exists("users")) {
      repository.delete("users");
    }
    Dataset<User> users = repository.create("users", descriptor);
    DatasetWriter<User> writer = users.newWriter();
    try {
      writer.open();
      for (User user : data) {
        writer.write(user);
      }
    } finally {
      writer.close();
    }
  }

  @AfterClass
  public static void tearDownClass() {
    server.close();
  }

  @Override
  public DatasetReader newReader() throws IOException {
    Dataset<User> dataset = repository.load("users");
    return dataset.newReader();
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
