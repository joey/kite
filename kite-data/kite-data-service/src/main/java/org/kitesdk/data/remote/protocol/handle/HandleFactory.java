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

package org.kitesdk.data.remote.protocol.handle;

import java.util.concurrent.atomic.AtomicLong;

public class HandleFactory {

  AtomicLong lastDatasetId;
  AtomicLong lastRefinableViewId;
  AtomicLong lastDatasetReaderId;
  AtomicLong lastDatasetWriterId;

  public HandleFactory() {
    lastDatasetId = new AtomicLong(0);
    lastRefinableViewId = new AtomicLong(0);
    lastDatasetReaderId = new AtomicLong(0);
    lastDatasetWriterId = new AtomicLong(0);
  }

  public DatasetHandle nextDatasetHandle() {
    DatasetHandle handle = new DatasetHandle();
    handle.setId(lastDatasetId.incrementAndGet());
    return handle;
  }

  public RefinableViewHandle nextRefinableViewHandle() {
    RefinableViewHandle handle = new RefinableViewHandle();
    handle.setId(lastRefinableViewId.incrementAndGet());
    return handle;
  }

  public DatasetReaderHandle nextDatasetReaderHandle() {
    DatasetReaderHandle handle = new DatasetReaderHandle();
    handle.setId(lastDatasetReaderId.incrementAndGet());
    return handle;
  }
  
  public DatasetWriterHandle nextDatasetWriterHandle() {
    DatasetWriterHandle handle = new DatasetWriterHandle();
    handle.setId(lastDatasetWriterId.incrementAndGet());
    return handle;
  }
}
