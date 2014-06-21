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
import java.util.Iterator;
import org.apache.avro.AvroRuntimeException;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.remote.protocol.RemoteDataProtocol;
import org.kitesdk.data.remote.protocol.handle.DatasetReaderHandle;
import org.slf4j.LoggerFactory;

public class RemoteDatasetReader<E> extends RemoteAvroClient implements DatasetReader<E> {

  private static final org.slf4j.Logger LOG = LoggerFactory
    .getLogger(RemoteDatasetReader.class);

  private RemoteDataProtocol<E> proxy;
  private DatasetReaderHandle handle;

  @SuppressWarnings("unchecked")
  public RemoteDatasetReader(RemoteDataProtocol<E> proxy, DatasetReaderHandle handle) throws IOException {
    this.proxy = proxy;
    this.handle = handle;
  }

  @Override
  public void open() {
    try {
      proxy.openReader(handle);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex; 
    }
  }

  @Override
  public boolean hasNext() {
    try {
      return proxy.hasNext(handle);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex; 
    }
  }

  @Override
  public E next() {
    try {
      return proxy.next(handle);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex;
    }
  }

  @Override
  public void remove() {
    try {
      proxy.remove(handle);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex; 
    }
  }

  @Override
  public void close() {
    try {
      proxy.closeReader(handle);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex; 
    }
  }

  @Override
  public boolean isOpen() {
    try {
      return proxy.isReaderOpen(handle);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex; 
    }
  }

  @Override
  public Iterator<E> iterator() {
    return this;
  }
}