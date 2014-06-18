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

package org.kitesdk.data.service;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.kitesdk.data.*;
import org.kitesdk.data.spi.AbstractDataset;
import org.kitesdk.data.spi.AbstractDatasetReader;
import org.kitesdk.data.spi.AbstractRefinableView;
import org.kitesdk.data.spi.Constraints;

public class MemoryDataset<E> extends AbstractDataset<E> {

  private List<E> data;
  private String name;
  private DatasetDescriptor descriptor;

  MemoryDataset(String name, DatasetDescriptor descriptor) {
    this.name = name;
    this.descriptor = descriptor;

    data = new ArrayList<E>();
  }

  @Override
  protected RefinableView<E> asRefinableView() {
    return this;
  }

  @Override
  public AbstractRefinableView<E> filter(Constraints c) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public DatasetDescriptor getDescriptor() {
    return descriptor;
  }

  @Override
  public Dataset<E> getPartition(PartitionKey key, boolean autoCreate) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void dropPartition(PartitionKey key) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Iterable<Dataset<E>> getPartitions() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public DatasetReader<E> newReader() {
    return new MemoryDatasetReader();
  }

  @Override
  public DatasetWriter<E> newWriter() {
    return new MemoryDatasetWriter();
  }

  public static class Builder {
    private String name;
    private DatasetDescriptor descriptor;

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder descriptor(DatasetDescriptor descriptor) {
      this.descriptor = descriptor;
      return this;
    }

    public <E> MemoryDataset<E> build() {
      Preconditions.checkState(this.name != null, "No dataset name defined");
      Preconditions.checkState(this.descriptor != null,
        "No dataset descriptor defined");

      return new MemoryDataset<E>(name, descriptor);
    }
  }

  private class MemoryDatasetReader extends AbstractDatasetReader<E> {

    private boolean open = false;
    private Iterator<E> iterator;

    @Override
    public void open() {
      if (open) {
        throw new IllegalStateException("Attempting to open an already open Reader");
      }
      open = true;
      iterator = data.iterator();
    }

    @Override
    public boolean hasNext() {
      Preconditions.checkState(open, "Attempting to read from "
          + "MemoryDatasetReader without call to open()");
      return iterator.hasNext();
    }

    @Override
    public E next() {
      Preconditions.checkState(open, "Attempting to read from "
          + "MemoryDatasetReader without call to open()");
      return iterator.next();
    }

    @Override
    public void close() {
      open = false;
    }

    @Override
    public boolean isOpen() {
      return open;
    }
  }

  private class MemoryDatasetWriter implements DatasetWriter<E> {

    private boolean open = false;

    @Override
    public void open() {
      open = true;
    }

    @Override
    public void write(E entity) {
      Preconditions.checkArgument(open, "Attempting to write to "
          + "MemoryDatasetWriter without call to open()");
      data.add(entity);
    }

    @Override
    public void flush() {
      Preconditions.checkArgument(open, "Attempting to write to "
          + "MemoryDatasetWriter without call to open()");
    }

    @Override
    public void close() {
      open = false;
    }

    @Override
    public boolean isOpen() {
      return open;
    }
  }
}
