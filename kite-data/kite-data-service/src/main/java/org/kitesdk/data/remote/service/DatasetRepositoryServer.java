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

package org.kitesdk.data.remote.service;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.reflect.ReflectResponder;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.remote.protocol.DatasetRepositoryProtocol;
import org.kitesdk.data.remote.protocol.handle.DatasetHandle;
import org.kitesdk.data.remote.protocol.handle.DatasetRepositoryHandle;
import org.kitesdk.data.remote.protocol.handle.HandleFactory;
import org.kitesdk.data.spi.DatasetRepositories;
import org.kitesdk.data.spi.DatasetRepository;
import org.slf4j.LoggerFactory;

public class DatasetRepositoryServer implements DatasetRepositoryProtocol {

  private static final org.slf4j.Logger LOG = LoggerFactory
    .getLogger(DatasetRepositoryServer.class);

  private final Map<DatasetRepositoryHandle, DatasetRepository> repositories;

  public DatasetRepositoryServer() {
    repositories = new HashMap<DatasetRepositoryHandle, DatasetRepository>();
  }

  @Override
  public DatasetHandle load(DatasetRepositoryHandle handle, String name) {
    Dataset<GenericData.Record> dataset = repositories.get(handle).load(name, GenericData.Record.class);
    return createDatasetServer(dataset);
  }

  @Override
  public DatasetHandle create(DatasetRepositoryHandle handle, String name, DatasetDescriptor descriptor) {
    Dataset<GenericData.Record> dataset = repositories.get(handle).create(name, descriptor);
    return createDatasetServer(dataset);
  }

  @Override
  public DatasetHandle update(DatasetRepositoryHandle handle, String name, DatasetDescriptor descriptor) {
    Dataset<GenericData.Record> dataset = repositories.get(handle).update(name, descriptor);
    return createDatasetServer(dataset);
  }

  private DatasetHandle createDatasetServer(Dataset<GenericData.Record> dataset) {
    Schema schema = dataset.getDescriptor().getSchema();
    DatasetServer<GenericData.Record> server = new DatasetServer<GenericData.Record>(dataset);
    int port = DatasetServer.startServer(server, 0).getPort();
    DatasetHandle handle = server.getRootHandle();
    handle.setPort(port);
    handle.setSchema(schema);
    return handle;
  }

  @Override
  public boolean delete(DatasetRepositoryHandle handle, String name) {
    return repositories.get(handle).delete(name);
  }

  @Override
  public boolean exists(DatasetRepositoryHandle handle, String name) {
    return repositories.get(handle).exists(name);
  }

  @Override
  public Collection<String> list(DatasetRepositoryHandle handle) {
    return repositories.get(handle).list();
  }

  @Override
  public URI getUri(DatasetRepositoryHandle handle) {
    return repositories.get(handle).getUri();
  }

  public static NettyServer startServer(int port) {
    return startServer(new DatasetRepositoryServer(), port);
  }

  private static NettyServer startServer(DatasetRepositoryServer repositoryServer,
      int port) {

    final NettyServer server = new NettyServer(
        new ReflectResponder(DatasetRepositoryProtocol.class, repositoryServer),
        new InetSocketAddress(port));
    server.start();
    return server;
  }

  public static void serve(int port) throws InterruptedException {
    serve(startServer(port));
  }

  private static void serve(final NettyServer server)
      throws InterruptedException {
    boolean wait = true;
    while (wait) {
      try {
        synchronized (server) {
          if (wait) {
            server.wait();
          }
        }
      } catch (InterruptedException ex) {
        wait = false;
        server.close();
        throw ex;
      }
    }
  }

  @Override
  public DatasetRepositoryHandle openRespository(String uri) {
    DatasetRepository repository = DatasetRepositories.repositoryFor(uri);
    DatasetRepositoryHandle handle = HandleFactory.nextDatasetRepositoryHandle();
    handle.setUri(repository.getUri().toString());
    repositories.put(handle, repository);

    return handle;
  }
}