/*
 * Copyright 2014 Cloudera.
 *
 * Licensed under the Apache License, Version 2.0 (RefinableViewHandle handle, the "License"); * you may not use this file except in compliance with the License.
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

import org.kitesdk.data.remote.protocol.handle.RefinableViewHandle;
import org.kitesdk.data.remote.protocol.handle.DatasetWriterHandle;
import org.kitesdk.data.remote.protocol.handle.DatasetReaderHandle;
import org.kitesdk.data.remote.protocol.handle.DatasetHandle;
import org.kitesdk.data.RefinableView;

/**
 * Protocol for remote {@link RefinableView}s.
 * 
 * @param <E> The type of entities stored in the {@code Dataset} underlying
 *            this {@code RefinableView}
 */
public interface RefinableViewProtocol<E> {
  
  /**
   * @see RefinableView#with(java.lang.String, java.lang.Object[])
   */
  public RefinableViewHandle with(RefinableViewHandle handle, String name, Object... values);

  /**
   * @see RefinableView#from(java.lang.String, java.lang.Comparable)
   */
  public RefinableViewHandle from(RefinableViewHandle handle, String name, Comparable value);

  /**
   * @see RefinableView#fromAfter(java.lang.String, java.lang.Comparable)
   */
  public RefinableViewHandle fromAfter(RefinableViewHandle handle, String name, Comparable value);

  /**
   * @see RefinableView#to(java.lang.String, java.lang.Comparable)
   */
  public RefinableViewHandle to(RefinableViewHandle handle, String name, Comparable value);

  /**
   * @see RefinableView#toBefore(java.lang.String, java.lang.Comparable)
   */
  public RefinableViewHandle toBefore(RefinableViewHandle handle, String name, Comparable value);

  /**
   * @see RefinableView#getDataset()
   */
  public DatasetHandle getDataset(RefinableViewHandle handle);

  /**
   * @see RefinableView#newReader()
   */
  public DatasetReaderHandle newReader(RefinableViewHandle handle);

  /**
   * @see RefinableView#newWriter()
   */
  public DatasetWriterHandle newWriter(RefinableViewHandle handle);

  /**
   * @see RefinableView#includes(java.lang.Object)
   */
  public boolean includes(RefinableViewHandle handle, E entity);

  /**
   * @see RefinableView#deleteAll()
   */
  public boolean deleteAll(RefinableViewHandle handle);

  /**
   * @see RefinableView#isEmpty()
   */
  public boolean isEmpty(RefinableViewHandle handle);
}