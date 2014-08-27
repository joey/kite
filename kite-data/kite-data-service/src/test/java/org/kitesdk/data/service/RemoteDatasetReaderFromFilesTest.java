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
import java.net.URI;
import org.apache.avro.ipc.NettyServer;
import org.junit.*;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.TestDatasetReaders;
import org.kitesdk.data.remote.service.DatasetRepositoryServer;
import static org.kitesdk.data.service.RemoteDatasetTestUtilities.*;
import org.kitesdk.data.spi.DatasetRepositories;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.filesystem.DatasetTestUtilities.RecordValidator;

public class RemoteDatasetReaderFromFilesTest extends TestDatasetReaders {

  static NettyServer server;
  static final int port = 42424;
  static final String datasetUri = "dataset:remote://localhost:"+port+"/dataset:file:/tmp/data/users";
  static final String repoUri = "repo:remote://localhost:"+port+"/repo:file:///tmp/data";

  @BeforeClass
  public static void setUpClass() throws IOException {
    server = DatasetRepositoryServer.startServer(port);

    DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
        .schema(User.class)
        .build();

    if (Datasets.exists(datasetUri)) {
      Datasets.delete(datasetUri);
    }
    Dataset<User> users = Datasets.create(datasetUri, descriptor, User.class);
    DatasetWriter<User> writer = users.newWriter();
    try {
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

  @Test
  public void testUri() {
    DatasetRepository repository = DatasetRepositories.repositoryFor(datasetUri);
    Assert.assertEquals("Repository URI doesn't match", URI.create(repoUri),
        repository.getUri());
  }

  @Override
  public DatasetReader newReader() throws IOException {
    Dataset<User> dataset = Datasets.load(datasetUri, User.class);
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
