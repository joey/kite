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

import java.net.InetSocketAddress;
import java.util.Iterator;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.reflect.ReflectResponder;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetReader;

public class DatasetReaderServer<E> implements DatasetReader<E> {

  private DatasetReader<E> datasetReader;

  DatasetReaderServer() {
  }

  DatasetReaderServer(DatasetReader<E> datasetReader) {
    this.datasetReader = datasetReader;
  }

  void setDatasetReader(DatasetReader<E> datasetReader) {
    this.datasetReader = datasetReader;
  }

  private void checkDatasetReader() throws NullPointerException {
    if (datasetReader == null) {
      throw new NullPointerException("No dataset reader to serve. "
          + "Create server with RemoteDatasetReader.Server(DatasetReader) "
          + "or call setDatasetReader(DatasetReader).");
    }
  }

  @Override
  public void open() {
    checkDatasetReader();
    datasetReader.open();
  }

  @Override
  public boolean hasNext() {
    checkDatasetReader();
    return datasetReader.hasNext();
  }

  @Override
  public E next() {
    checkDatasetReader();
    return datasetReader.next();
  }

  @Override
  public void remove() {
    checkDatasetReader();
    datasetReader.remove();
  }

  @Override
  public void close() {
    checkDatasetReader();
    datasetReader.close();
  }

  @Override
  public boolean isOpen() {
    checkDatasetReader();
    return datasetReader.isOpen();
  }

  @Override
  public Iterator<E> iterator() {
    checkDatasetReader();
    return datasetReader.iterator();
  }

  public static <E> NettyServer startServer(Dataset<E> dataset, Class<E> type,
      int port) {
    final NettyServer server = new NettyServer(
        new ReflectResponder(DatasetReader.class,
        new DatasetReaderServer<E>(dataset.newReader()),
        new ServiceReflectData(DatasetReader.class, type)),
        new InetSocketAddress(port));
    server.start();
    return server;
  }

  public static <E> void serve(Dataset<E> dataset, Class<E> type, int port)
      throws InterruptedException {
    final NettyServer server = startServer(dataset, type, port);
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
