/*
 * Copyright 2014 joey.
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
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.PartitionKey;
import org.kitesdk.data.remote.protocol.RemoteDataProtocol;
import org.kitesdk.data.remote.protocol.handle.DatasetHandle;
import org.slf4j.LoggerFactory;

public class RemoteDataset<E> extends RemoteRefinableView<E> implements Dataset<E> {

  private static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(RemoteDataset.class);

  private RemoteDataProtocol<E> proxy;
  private DatasetHandle handle;
  private Class<E> type;

  @SuppressWarnings("unchecked")
  public RemoteDataset(RemoteDataProtocol<E> proxy, DatasetHandle handle, Class<E> type) throws IOException {
    super(proxy, handle, type);
    this.proxy = proxy;
    this.handle = handle;
    this.type = type;
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

  @Override
  public Dataset<E> getPartition(PartitionKey key, boolean autoCreate) {
    try {
      DatasetHandle partitionHandle = proxy.getPartition(handle, key, autoCreate);
      return new RemoteDataset<E>(proxy, partitionHandle, type);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex;
    } catch (IOException ex) {
      throw new RuntimeException("IOException while creating Dataset",ex);
    }
  }

  @Override
  public void dropPartition(PartitionKey key) {
    try {
      proxy.dropPartition(handle, key);
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex;
    }
  }

  @Override
  public Iterable<Dataset<E>> getPartitions() {
    try {
      final Iterable<DatasetHandle> partitions = proxy.getPartitions(handle);
      return new Iterable<Dataset<E>>() {

        @Override
        public Iterator<Dataset<E>> iterator() {
          final Iterator<DatasetHandle> iterator = partitions.iterator();

          return new Iterator<Dataset<E>>() {

            @Override
            public boolean hasNext() {
              return iterator.hasNext();
            }

            @Override
            public Dataset<E> next() {
              DatasetHandle partition = iterator.next();
              if (partition == null) {
                return null;
              }
              try {
                return new RemoteDataset<E>(proxy, partition, type);
              } catch (IOException ex) {
                throw new RuntimeException("IOException while creating Dataset", ex);
              }
            }

            @Override
            public void remove() {
              iterator.remove();
            }
          };
        }
      };
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex;
    }
  }

}
