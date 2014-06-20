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
import org.apache.avro.AvroRuntimeException;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.remote.protocol.RemoteDataProtocol;
import org.kitesdk.data.remote.protocol.handle.DatasetWriterHandle;
import org.slf4j.LoggerFactory;

public class RemoteDatasetWriter<E> extends RemoteAvroClient implements DatasetWriter<E> {

  private static final org.slf4j.Logger LOG = LoggerFactory
    .getLogger(RemoteDatasetWriter.class);

  private RemoteDataProtocol<E> proxy;
  private DatasetWriterHandle handle;

  @SuppressWarnings("unchecked")
  public RemoteDatasetWriter(RemoteDataProtocol<E> proxy, DatasetWriterHandle handle, Class<E> type) throws IOException {
    this.proxy = proxy;
    this.handle = handle;
  }

  @Override
  public void open() {
    try {
      proxy.openWriter(handle);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex; 
    }
  }

  @Override
  public void write(E entity) {
    try {
      proxy.write(handle, entity);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex; 
    }
  }

  @Override
  public void flush() {
    try {
      proxy.flush(handle);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex; 
    }
  }


  @Override
  public void close() {
    try {
      proxy.closeWriter(handle);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex; 
    }
  }

  @Override
  public boolean isOpen() {
    try {
      return proxy.isWriterOpen(handle);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex; 
    }
  }
}