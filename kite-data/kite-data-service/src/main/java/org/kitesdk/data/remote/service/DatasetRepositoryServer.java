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
import org.apache.avro.Schema;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.reflect.ReflectResponder;
import org.kitesdk.data.*;
import org.kitesdk.data.remote.protocol.DatasetRepositoryProtocol;
import org.kitesdk.data.remote.protocol.handle.DatasetHandle;
import org.slf4j.LoggerFactory;

public class DatasetRepositoryServer implements DatasetRepositoryProtocol {

  private static final org.slf4j.Logger LOG = LoggerFactory
    .getLogger(DatasetRepositoryServer.class);

  private DatasetRepository repository;

  public DatasetRepositoryServer(String uri) {
    this(DatasetRepositories.open(uri));
  }

  public DatasetRepositoryServer(URI uri) {
    this(DatasetRepositories.open(uri));
  }

  public DatasetRepositoryServer(DatasetRepository repository) {
    this.repository = repository;
  }

  @Override
  public DatasetHandle load(String name) {
    Dataset<Object> dataset = repository.load(name);
    return createDatasetServer(dataset);
  }

  @Override
  public DatasetHandle create(String name, DatasetDescriptor descriptor) {
    Dataset<Object> dataset = repository.create(name, descriptor);
    return createDatasetServer(dataset);
  }

  @Override
  public DatasetHandle update(String name, DatasetDescriptor descriptor) {
    Dataset<Object> dataset = repository.update(name, descriptor);
    return createDatasetServer(dataset);
  }

  private DatasetHandle createDatasetServer(Dataset<Object> dataset) {
    Schema schema = dataset.getDescriptor().getSchema();
    DatasetServer<Object> server = new DatasetServer<Object>(dataset);
    int port = DatasetServer.startServer(server, 0).getPort();
    DatasetHandle handle = server.getRootHandle();
    handle.setPort(port);
    handle.setSchema(schema);
    return handle;
  }

  @Override
  public boolean delete(String name) {
    return repository.delete(name);
  }

  @Override
  public boolean exists(String name) {
    return repository.exists(name);
  }

  @Override
  public Collection<String> list() {
    return repository.list();
  }

  @Override
  public URI getUri() {
    return repository.getUri();
  }

  public static NettyServer startServer(String uri, int port) {
    return startServer(new DatasetRepositoryServer(uri), port);
  }
  public static NettyServer startServer(URI uri, int port) {
    return startServer(new DatasetRepositoryServer(uri), port);
  }

  public static NettyServer startServer(DatasetRepository repository, int port) {
    return startServer(new DatasetRepositoryServer(repository), port);
  }

  private static NettyServer startServer(DatasetRepositoryServer repositoryServer,
      int port) {

    final NettyServer server = new NettyServer(
        new ReflectResponder(DatasetRepositoryProtocol.class, repositoryServer),
        new InetSocketAddress(port));
    server.start();
    return server;
  }

  public static void serve(String uri, int port) throws InterruptedException {
    serve(startServer(uri, port));
  }

  public static void serve(URI uri, int port) throws InterruptedException {
    serve(startServer(uri, port));
  }

  public static void serve(DatasetRepository repository, int port) throws InterruptedException {
    serve(startServer(repository, port));
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
}