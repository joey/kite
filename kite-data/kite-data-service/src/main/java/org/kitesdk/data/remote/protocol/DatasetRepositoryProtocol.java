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

import java.net.URI;
import java.util.Collection;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.remote.protocol.handle.DatasetHandle;
import org.kitesdk.data.remote.protocol.handle.DatasetRepositoryHandle;
import org.kitesdk.data.spi.DatasetRepository;

/**
 * Protocol for a remote {@link DatasetRepository}.
 */
public interface DatasetRepositoryProtocol {

  DatasetRepositoryHandle openRespository(String uri);

  /**
   * @see DatasetRepository#load(java.lang.String)
   */
  DatasetHandle load(DatasetRepositoryHandle handle, String name);

  /**
   * @see DatasetRepository#create(java.lang.String, org.kitesdk.data.DatasetDescriptor)
   */
  DatasetHandle create(DatasetRepositoryHandle handle, String name,
      DatasetDescriptor descriptor);

  /**
   * @see DatasetRepository#update(java.lang.String, org.kitesdk.data.DatasetDescriptor)
   */
  DatasetHandle update(DatasetRepositoryHandle handle, String name,
      DatasetDescriptor descriptor);

  /**
   * @see DatasetRepository#delete(java.lang.String)
   */
  boolean delete(DatasetRepositoryHandle handle, String name);

  /**
   * @see DatasetRepository#exists(java.lang.String)
   */
  boolean exists(DatasetRepositoryHandle handle, String name);

  /**
   * @see DatasetRepository#list()
   */
  Collection<String> list(DatasetRepositoryHandle handle);

  /**
   * @see DatasetRepository#getUri()
   */
  URI getUri(DatasetRepositoryHandle handle);
}