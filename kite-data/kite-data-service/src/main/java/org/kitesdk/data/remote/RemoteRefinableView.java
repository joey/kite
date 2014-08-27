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
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.RefinableView;
import org.kitesdk.data.remote.protocol.RemoteDataProtocol;
import org.kitesdk.data.remote.protocol.handle.DatasetReaderHandle;
import org.kitesdk.data.remote.protocol.handle.DatasetWriterHandle;
import org.kitesdk.data.remote.protocol.handle.RefinableViewHandle;
import org.slf4j.LoggerFactory;

public class RemoteRefinableView<E> extends RemoteAvroClient implements RefinableView<E> {

  private static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(RemoteRefinableView.class);

  private final RemoteDataProtocol<E> proxy;
  private final RefinableViewHandle handle;
  private final Schema schema;
  private final Class<E> type;
  private final URI uri;

  @SuppressWarnings("unchecked")
  public RemoteRefinableView(RemoteDataProtocol<E> proxy,
      RefinableViewHandle handle, Schema schema, Class<E> type) throws IOException {
    this.proxy = proxy;
    this.handle = handle;
    this.schema = schema;
    this.type = type;

    try {
      String scheme = "view";
      if (handle.getUri().startsWith("dataset")) {
        scheme = "dataset";
      }
      uri = new URI(scheme+":remote:"+handle.getUri());
    } catch (URISyntaxException ex) {
      throw new DatasetException(ex);
    }
  }

  @Override
  public RefinableView<E> with(String name, Object... values) {
    try {
      RefinableViewHandle viewHandle = proxy.with(handle, name, values);
      return new RemoteRefinableView<E>(proxy, viewHandle, schema, type);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex;
    } catch (IOException ex) {
      throw new RuntimeException("IOException while creating RefinableView",ex);
    }
  }

  @Override
  public RefinableView<E> from(String name, Comparable value) {
    try {
      RefinableViewHandle viewHandle = proxy.from(handle, name, value);
      return new RemoteRefinableView<E>(proxy, viewHandle, schema, type);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex;
    } catch (IOException ex) {
      throw new RuntimeException("IOException while creating RefinableView",ex);
    }
  }

  @Override
  public RefinableView<E> fromAfter(String name, Comparable value) {
    try {
      RefinableViewHandle viewHandle = proxy.fromAfter(handle, name, value);
      return new RemoteRefinableView<E>(proxy, viewHandle, schema, type);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex;
    } catch (IOException ex) {
      throw new RuntimeException("IOException while creating RefinableView",ex);
    }
  }

  @Override
  public RefinableView<E> to(String name, Comparable value) {
    try {
      RefinableViewHandle viewHandle = proxy.to(handle, name, value);
      return new RemoteRefinableView<E>(proxy, viewHandle, schema, type);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex;
    } catch (IOException ex) {
      throw new RuntimeException("IOException while creating RefinableView",ex);
    }
  }

  @Override
  public RefinableView<E> toBefore(String name, Comparable value) {
    try {
      RefinableViewHandle viewHandle = proxy.toBefore(handle, name, value);
      return new RemoteRefinableView<E>(proxy, viewHandle, schema, type);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex;
    } catch (IOException ex) {
      throw new RuntimeException("IOException while creating RefinableView",ex);
    }
  }

  @Override
  public Dataset<E> getDataset() {
    if (this instanceof Dataset) {
      return (Dataset<E>)this;
    }
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public DatasetReader<E> newReader() {
    try {
      DatasetReaderHandle readerHandle = proxy.newReader(handle);
      return new RemoteDatasetReader<E>(proxy, readerHandle);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex;
    } catch (IOException ex) {
      throw new RuntimeException("IOException while creating Reader",ex);
    }
  }

  @Override
  public DatasetWriter<E> newWriter() {
    try {
      DatasetWriterHandle writerHandle = proxy.newWriter(handle);
      return new RemoteDatasetWriter<E>(proxy, writerHandle);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex;
    } catch (IOException ex) {
      throw new RuntimeException("IOException while creating Writer",ex);
    }
  }

  @Override
  public boolean includes(E entity) {
    try {
      return proxy.includes(handle, entity);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex;
    }
  }

  @Override
  public boolean deleteAll() {
    try {
      return proxy.deleteAll(handle);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex;
    }
  }

  @Override
  public Class<E> getType() {
    return type;
  }

  @Override
  public boolean isEmpty() {
    try {
      return proxy.isEmpty(handle);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex;
    }
  }

  @Override
  public URI getUri() {
    return uri;
  }
}
