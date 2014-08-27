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

import com.google.common.base.Preconditions;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.RefinableView;
import org.kitesdk.data.spi.AbstractDataset;
import org.kitesdk.data.spi.AbstractDatasetReader;
import org.kitesdk.data.spi.AbstractRefinableView;
import org.kitesdk.data.spi.Constraints;

public class MemoryDataset<E> extends AbstractDataset<E> {

  private final List<E> data;
  private final String name;
  private final DatasetDescriptor descriptor;
  private final URI uri;

  MemoryDataset(String name, DatasetDescriptor descriptor, List<E> data,
      Class<E> type) {
    super(type, descriptor.getSchema());
    this.name = name;
    this.descriptor = descriptor;

    this.data = new ArrayList<E>();
    if (data != null) {
      this.data.addAll(data);
    }
    try {
      uri = new URI("dataset:memory:/"+this.hashCode());
    } catch (URISyntaxException ex) {
      throw new DatasetException(ex);
    }
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
  public DatasetReader<E> newReader() {
    return new MemoryDatasetReader();
  }

  @Override
  public DatasetWriter<E> newWriter() {
    return new MemoryDatasetWriter();
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public boolean isEmpty() {
    return data.isEmpty();
  }

  public static class Builder {
    private String name;
    private DatasetDescriptor descriptor;
    private List data;
    private Class type;

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder descriptor(DatasetDescriptor descriptor) {
      this.descriptor = descriptor;
      return this;
    }

    public <E> Builder data(List<E> data) {
      this.data = data;
      return this;
    }

    public Builder type(Class type) {
      this.type = type;
      return this;
    }

    @SuppressWarnings("unchecked")
    public <E> MemoryDataset<E> build() {
      Preconditions.checkState(this.name != null, "No dataset name defined");
      Preconditions.checkState(this.descriptor != null,
        "No dataset descriptor defined");

      return new MemoryDataset<E>(name, descriptor, data, type);
    }
  }

  private class MemoryDatasetReader extends AbstractDatasetReader<E> {

    private boolean open = false;
    private final Iterator<E> iterator;

    public MemoryDatasetReader() {
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

    @Override
    public void initialize() {
    }
  }

  private class MemoryDatasetWriter implements DatasetWriter<E> {

    private boolean open = true;

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

    @Override
    public void sync() {
      Preconditions.checkArgument(open, "Attempting to write to "
          + "MemoryDatasetWriter without call to open()");
    }
  }
}
