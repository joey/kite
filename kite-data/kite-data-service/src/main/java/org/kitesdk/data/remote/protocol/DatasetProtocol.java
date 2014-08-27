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

import org.kitesdk.data.remote.protocol.handle.DatasetHandle;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;

/**
 * Protocol for remote {@link Dataset}s.
 * 
 * @param <E> The type of entities stored in this {@code Dataset}.
 */
public interface DatasetProtocol<E> extends RefinableViewProtocol<E> {

  /**
   * @see Dataset#getName()
   */
  String getName(DatasetHandle handle);

  /**
   * @see Dataset#getDescriptor()
   */
  DatasetDescriptor getDescriptor(DatasetHandle handle);

}