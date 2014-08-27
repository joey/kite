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
package org.kitesdk.data.remote.protocol;

import org.kitesdk.data.remote.protocol.handle.DatasetReaderHandle;
import java.util.Iterator;
import org.kitesdk.data.DatasetReader;

/**
 * Protocol for remote {@link DatasetReader}'s.
 * 
 * @param <E> The type of entity produced by this reader.
 */
public interface DatasetReaderProtocol<E> {

  /**
   * @see DatasetReader#hasNext()
   */
  public boolean hasNext(DatasetReaderHandle handle);

  /**
   * @see DatasetReader#next()
   */
  public E next(DatasetReaderHandle handle); 

  /**
   * @see DatasetReader#remove()
   */
  public void remove(DatasetReaderHandle handle); 

  /**
   * @see DatasetReader#close()
   */
  public void closeReader(DatasetReaderHandle handle);

  /**
   * @see DatasetReader#isOpen()
   */
  public boolean isReaderOpen(DatasetReaderHandle handle);

  /**
   * @see DatasetReader#iterator()
   */
  public Iterator<E> iterator(DatasetReaderHandle handle);
  
}