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

package org.kitesdk.data.remote.service;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.reflect.ReflectResponder;
import org.kitesdk.data.*;
import org.kitesdk.data.remote.protocol.RemoteDataProtocol;
import org.kitesdk.data.remote.protocol.handle.DatasetHandle;
import org.kitesdk.data.remote.protocol.handle.DatasetReaderHandle;
import org.kitesdk.data.remote.protocol.handle.DatasetWriterHandle;
import org.kitesdk.data.remote.protocol.handle.HandleFactory;
import org.kitesdk.data.remote.protocol.handle.RefinableViewHandle;
import org.slf4j.LoggerFactory;

public class DatasetServer<E> implements RemoteDataProtocol<E> {

  private static final org.slf4j.Logger LOG = LoggerFactory
    .getLogger(DatasetServer.class);

  private Map<DatasetHandle, Dataset<E>> datasets;
  private Map<RefinableViewHandle, RefinableView<E>> views;
  private Map<DatasetReaderHandle, DatasetReader<E>> readers;
  private Map<DatasetWriterHandle, DatasetWriter<E>> writers;
  private HandleFactory handleFactory;
  private DatasetHandle rootHandle;

  public DatasetServer(Dataset<E> dataset) {
    datasets = new HashMap<DatasetHandle, Dataset<E>>();
    views = new HashMap<RefinableViewHandle, RefinableView<E>>();
    readers = new HashMap<DatasetReaderHandle, DatasetReader<E>>();
    writers = new HashMap<DatasetWriterHandle, DatasetWriter<E>>();
    handleFactory = new HandleFactory();

    rootHandle = put(dataset);
    put((RefinableView<E>)dataset);
  }

  protected Dataset<E> get(DatasetHandle handle) {
    Dataset<E> dataset = datasets.get(handle);
    if (dataset == null) {
      throw new NullPointerException(String.format("No Dataset for handle %s",
          handle.toString()));
    }
    return dataset;
  }

  protected RefinableView<E> get(RefinableViewHandle handle) {
    RefinableView<E> view = views.get(handle);
    if (view == null) {
      NullPointerException npe = new NullPointerException(String.format(
          "No RefinableView for handle %s", handle.toString()));
      LOG.warn("No handle in map " + views.toString(), npe);
    }
    return view;
  }

  protected DatasetReader<E> get(DatasetReaderHandle handle) {
    DatasetReader<E> reader = readers.get(handle);
    if (reader == null) {
      throw new NullPointerException(String.format("No DatasetReader for handle %s",
          handle.toString()));
    }
    return reader;
  }

  protected DatasetWriter<E> get(DatasetWriterHandle handle) {
    DatasetWriter<E> writer = writers.get(handle);
    if (writer == null) {
      throw new NullPointerException(String.format("No DatasetWriter for handle %s",
          handle.toString()));
    }
    return writer;
  }

  protected final DatasetHandle put(Dataset<E> dataset) {
    DatasetHandle handle = handleFactory.nextDatasetHandle();
    datasets.put(handle, dataset);
    return handle;
  }

  protected final RefinableViewHandle put(RefinableView<E> view) {
    RefinableViewHandle handle = handleFactory.nextRefinableViewHandle();
    views.put(handle, view);
    return handle;
  }

  protected DatasetReaderHandle put(DatasetReader<E> reader) {
    DatasetReaderHandle handle = handleFactory.nextDatasetReaderHandle();
    readers.put(handle, reader);
    return handle;
  }

  protected DatasetWriterHandle put(DatasetWriter<E> writer) {
    DatasetWriterHandle handle = handleFactory.nextDatasetWriterHandle();
    writers.put(handle, writer);
    return handle;
  }

  @Override
  public DatasetHandle getRootHandle() {
    return rootHandle;
  }

  @Override
  public String getName(DatasetHandle handle) {
      return get(handle).getName();
  }

  @Override
  public DatasetDescriptor getDescriptor(DatasetHandle handle) {
    return get(handle).getDescriptor();
  }

  @Override
  public DatasetHandle getPartition(DatasetHandle handle, PartitionKey key, boolean autoCreate) {
    Dataset<E> partition = get(handle).getPartition(key, autoCreate);
    return put(partition);
  }

  @Override
  public void dropPartition(DatasetHandle handle, PartitionKey key) {
    get(handle).dropPartition(key);
  }

  @Override
  public Iterable<DatasetHandle> getPartitions(DatasetHandle handle) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public RefinableViewHandle with(RefinableViewHandle handle, String name, Object... values) {
    RefinableView<E> view = get(handle).with(name, values);
    return put(view);
  }

  @Override
  public RefinableViewHandle from(RefinableViewHandle handle, String name, Comparable value) {
    RefinableView<E> view = get(handle).from(name, value);
    return put(view);
  }

  @Override
  public RefinableViewHandle fromAfter(RefinableViewHandle handle, String name, Comparable value) {
    RefinableView<E> view = get(handle).fromAfter(name, value);
    return put(view);
  }

  @Override
  public RefinableViewHandle to(RefinableViewHandle handle, String name, Comparable value) {
    RefinableView<E> view = get(handle).to(name, value);
    return put(view);
  }

  @Override
  public RefinableViewHandle toBefore(RefinableViewHandle handle, String name, Comparable value) {
    RefinableView<E> view = get(handle).toBefore(name, value);
    return put(view);
  }

  @Override
  public DatasetHandle getDataset(RefinableViewHandle handle) {
    //Dataset<E> dataset = get(handle).getDataset();
    // Look for dataset in the map?
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public DatasetReaderHandle newReader(RefinableViewHandle handle) {
    DatasetReader<E> reader = get(handle).newReader();
    return put(reader);
  }

  @Override
  public DatasetWriterHandle newWriter(RefinableViewHandle handle) {
    DatasetWriter<E> writer = get(handle).newWriter();
    return put(writer);
  }

  @Override
  public boolean includes(RefinableViewHandle handle, E entity) {
    return get(handle).includes(entity);
  }

  @Override
  public boolean deleteAll(RefinableViewHandle handle) {
    return get(handle).deleteAll();
  }

  @Override
  public void openReader(DatasetReaderHandle handle) {
    get(handle).open();
  }

  @Override
  public boolean hasNext(DatasetReaderHandle handle) {
    return get(handle).hasNext();
  }

  @Override
  public E next(DatasetReaderHandle handle) {
    return get(handle).next();
  }

  @Override
  public void remove(DatasetReaderHandle handle) {
    get(handle).remove();
  }

  @Override
  public void closeReader(DatasetReaderHandle handle) {
    get(handle).close();
  }

  @Override
  public boolean isReaderOpen(DatasetReaderHandle handle) {
    return get(handle).isOpen();
  }

  @Override
  public Iterator<E> iterator(DatasetReaderHandle handle) {
    return get(handle).iterator();
  }

  @Override
  public void openWriter(DatasetWriterHandle handle) {
    get(handle).open();
  }

  @Override
  public void write(DatasetWriterHandle handle, E entity) {
    get(handle).write(entity);
  }

  @Override
  public void flush(DatasetWriterHandle handle) {
    get(handle).flush();
  }

  @Override
  public void closeWriter(DatasetWriterHandle handle) {
    get(handle).close();
  }

  @Override
  public boolean isWriterOpen(DatasetWriterHandle handle) {
    return get(handle).isOpen();
  }

  private Schema getSchema() {
    return get(rootHandle).getDescriptor().getSchema();
  }

  public static <E> NettyServer startServer(Dataset<E> dataset, int port) {

    return startServer(new DatasetServer<E>(dataset), port);
  }

  public static <E> NettyServer startServer(DatasetServer<E> dataServer, int port) {
    Schema schema = dataServer.getSchema();
    ServiceReflectData data = new ServiceReflectData(RemoteDataProtocol.class,
        schema);

    final NettyServer server = new NettyServer(
        new ServiceReflectResponder(RemoteDataProtocol.class, dataServer, data,
            schema),
        new InetSocketAddress(port));
    server.start();
    return server;
  }

  public static <E> void serve(Dataset<E> dataset, int port)
      throws InterruptedException {
    serve(startServer(dataset, port));
  }

  public static <E> void serve(DatasetServer<E> dataServer, int port)
      throws InterruptedException {
    serve(startServer(dataServer, port));
  }

  private static <E> void serve(final NettyServer server)
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