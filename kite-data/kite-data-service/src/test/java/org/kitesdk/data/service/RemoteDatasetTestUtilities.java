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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.junit.Assert;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetWriter;

public class RemoteDatasetTestUtilities {

  public static List<User> data = Arrays.asList(
      new User("Joey", "blue"),
      new User("Sean", "green"),
      new User("Alex", "red"),
      new User("Ryan", "orange"),
      new User("Tom", "black"));

  public static MemoryDataset<User> createEmptyMemoryDataset() {
    return new MemoryDataset.Builder().name("users").
        descriptor(new DatasetDescriptor.Builder().schema(User.class).build()).
        build();
  }

  public static MemoryDataset<User> createMemoryDataset() {
    return createMemoryDataset(data);
  }

  public static MemoryDataset<User> createMemoryDataset(List<User> data) {
    MemoryDataset<User> dataset = createEmptyMemoryDataset();

    DatasetWriter<User> writer = dataset.newWriter();
    checkWriterBehavior(writer, data);

    return dataset;
  }

  public static <E> void checkWriterBehavior(DatasetWriter<E> writer,
      Collection<E> data) {
    try {
      Assert.assertTrue("Writer is not open", writer.isOpen());

      for (E entity : data) {
        writer.write(entity);
      }

    } finally {
      writer.close();
    }

    Assert.assertFalse("Writer is open after close()", writer.isOpen());
  }
}