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
import org.apache.avro.Schema;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.remote.protocol.RemoteDataProtocol;
import org.kitesdk.data.remote.protocol.handle.DatasetHandle;
import org.slf4j.LoggerFactory;

public class RemoteDataset<E> extends RemoteRefinableView<E> implements Dataset<E> {

  private static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(RemoteDataset.class);

  private final RemoteDataProtocol<E> proxy;
  private final DatasetHandle handle;

  @SuppressWarnings("unchecked")
  public RemoteDataset(RemoteDataProtocol<E> proxy, DatasetHandle handle,
      Schema schema, Class<E> type) throws IOException {
    super(proxy, handle, schema, type);
    this.proxy = proxy;
    this.handle = handle;
  }

  @Override
  public String getName() {
    try {
      return proxy.getName(handle);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex;
    }
  }

  @Override
  public DatasetDescriptor getDescriptor() {
    try {
      return proxy.getDescriptor(handle);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex;
    }
  }
}
