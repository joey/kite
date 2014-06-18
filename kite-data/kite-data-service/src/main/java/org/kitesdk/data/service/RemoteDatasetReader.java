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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.reflect.ReflectRequestor;
import org.kitesdk.data.DatasetReader;
import org.slf4j.LoggerFactory;

public class RemoteDatasetReader<E> implements DatasetReader<E> {

  private static final org.slf4j.Logger LOG = LoggerFactory
    .getLogger(RemoteDatasetReader.class);

  private NettyTransceiver client;
  private DatasetReader<E> proxy;

  @SuppressWarnings("unchecked")
  public RemoteDatasetReader(String hostname, Class<E> type, int port) throws IOException {
    client = new NettyTransceiver(new InetSocketAddress(hostname, port));
    proxy = ReflectRequestor.getClient(DatasetReader.class, client,
        new ServiceReflectData(DatasetReader.class, type));
  }

  @Override
  public void open() {
    try {
      proxy.open();
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex; 
    }
  }

  @Override
  public boolean hasNext() {
    try {
      return proxy.hasNext();
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex; 
    }
  }

  @Override
  public E next() {
    try {
      return proxy.next();
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex;
    }
  }

  @Override
  public void remove() {
    try {
      proxy.remove();
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex; 
    }
  }

  @Override
  public void close() {
    try {
      proxy.close();
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex; 
    }
  }

  @Override
  public boolean isOpen() {
    try {
      return proxy.isOpen();
    } catch (AvroRuntimeException ex) {
      handleAvroRuntimeException(ex);
      throw ex; 
    }
  }

  @Override
  public Iterator<E> iterator() {
    return this;
  }

  @SuppressWarnings("unchecked")
  private void handleAvroRuntimeException(AvroRuntimeException ex)
      throws RuntimeException {
    Throwable cause = ex.getCause();
    if (cause instanceof RuntimeException) {
      throw (RuntimeException)cause;
    } else {
      String message = ex.getMessage();
      String[] parts = message.split(":", 2);
      String className = parts[0];
      if (parts.length == 2) {
        message = parts[1].trim();
      } else {
        message = null;
      }
      Class<?> clazz;
      try {
        clazz = Class.forName(className);
        if (RuntimeException.class.isAssignableFrom(clazz)) {
          try {
            if (message == null) {
              RuntimeException rex = ((Class<RuntimeException>)clazz).newInstance();
              throw rex;
            } else {
              Constructor<RuntimeException> declaredConstructor =
                  ((Class<RuntimeException>)clazz).getDeclaredConstructor(String.class);
              RuntimeException rex = declaredConstructor.newInstance(message);
              throw rex;
            }
          } catch (NoSuchMethodException ex1) {
            try {
              RuntimeException rex = ((Class<RuntimeException>)clazz).newInstance();
              throw rex;
            } catch (InstantiationException ex2) {
            } catch (IllegalAccessException ex2) {
            }
          } catch (InstantiationException ex1) {
          } catch (IllegalAccessException ex1) {
          } catch (IllegalArgumentException ex1) {
          } catch (InvocationTargetException ex1) {
          }
        }
      } catch (ClassNotFoundException ex1) {
      }
    }
  }
}
