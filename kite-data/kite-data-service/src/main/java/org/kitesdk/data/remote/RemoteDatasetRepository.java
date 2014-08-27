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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.reflect.ReflectRequestor;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.remote.protocol.DatasetRepositoryProtocol;
import org.kitesdk.data.remote.protocol.RemoteDataProtocol;
import org.kitesdk.data.remote.protocol.handle.DatasetHandle;
import org.kitesdk.data.remote.protocol.handle.DatasetRepositoryHandle;
import org.kitesdk.data.remote.service.ServiceReflectData;

public class RemoteDatasetRepository extends RemoteAvroClient implements DatasetRepository {

  private final DatasetRepositoryProtocol proxy;
  private final String hostname;
  private final DatasetRepositoryHandle repoHandle;
  private final URI uri;

  public RemoteDatasetRepository(String hostname, int port, String remoteUri)
      throws IOException {
    this.hostname = hostname;
    NettyTransceiver client = new NettyTransceiver(
        new InetSocketAddress(hostname, port));
    proxy = ReflectRequestor.getClient(DatasetRepositoryProtocol.class, client);
    repoHandle = proxy.openRespository(remoteUri);

    try {
      this.uri = new URI("repo:remote://" + hostname + ':' + port + "/" + repoHandle.getUri());
    } catch (URISyntaxException ex) {
      throw new DatasetException(ex);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <E> Dataset<E> load(String name) {
    return (Dataset<E>) load(name, Object.class);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <E> Dataset<E> create(String name, DatasetDescriptor descriptor) {
    return (Dataset<E>) create(name, descriptor, Object.class);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <E> Dataset<E> update(String name, DatasetDescriptor descriptor) {
    return (Dataset<E>) update(name, descriptor, Object.class);
  }

  private <E> Dataset<E> createDataset(DatasetHandle handle, Class<E> type) {
    try {
      String host = handle.getHostname() != null ? handle.getHostname()
          : hostname;
      int port = handle.getPort();
      Schema schema = handle.getSchema();
      NettyTransceiver client = new NettyTransceiver(new InetSocketAddress(host, port));
      @SuppressWarnings("unchecked")
      RemoteDataProtocol<E> datasetProxy = ReflectRequestor.getClient(
          RemoteDataProtocol.class, client,
          new ServiceReflectData(RemoteDataProtocol.class, schema));
      return new RemoteDataset<E>(datasetProxy, handle, schema, type);
    } catch (IOException ex) {
      throw new RuntimeException("IOException creating RemoteDataset", ex);
    }
  }

  @Override
  public boolean delete(String name) {
    try {
      return proxy.delete(repoHandle, name);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex;
    }
  }

  @Override
  public boolean exists(String name) {
    try {
      return proxy.exists(repoHandle, name);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex;
    }
  }

  @Override
  public Collection<String> list() {
    try {
      return proxy.list(repoHandle);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex;
    }
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public <E> Dataset<E> load(String name, Class<E> type) {
    try {
      DatasetHandle handle = proxy.load(repoHandle, name);
      return createDataset(handle, type);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex;
    }
  }

  @Override
  public <E> Dataset<E> create(String name, DatasetDescriptor descriptor, Class<E> type) {
    try {
      DatasetHandle handle = proxy.create(repoHandle, name, descriptor);
      return createDataset(handle, type);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex;
    }
  }

  @Override
  public <E> Dataset<E> update(String name, DatasetDescriptor descriptor, Class<E> type) {
    try {
      DatasetHandle handle = proxy.update(repoHandle, name, descriptor);
      return createDataset(handle, type);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex;
    }
  }
}
