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
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.junit.*;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.TestDatasetReaders;
import org.kitesdk.data.spi.filesystem.DatasetTestUtilities.RecordValidator;

public class MemoryDatasetReaderTest extends TestDatasetReaders {

  static MemoryDataset<User> dataset;
  static List<User> data = Arrays.asList(
      new User("Joey", "blue"),
      new User("Sean", "green"),
      new User("Alex", "red"),
      new User("Ryan", "orange"),
      new User("Tom", "black"));

  @BeforeClass
  public static void setUpClass() {
    dataset = new MemoryDataset.Builder().name("users").
        descriptor(new DatasetDescriptor.Builder().schema(User.class).build()).
        build();
    DatasetWriter<User> writer = dataset.newWriter();
    writer.open();
    for (User u : data) {
      writer.write(u);
    }
    writer.close();
  }

  @Override
  public DatasetReader newReader() throws IOException {
    return dataset.newReader();
  }

  @Override
  public int getTotalRecords() {
    return 5;
  }

  @Override
  public RecordValidator getValidator() {
    return new MyRecordValidator();
  }

  public class MyRecordValidator implements RecordValidator<User> {

    @Override
    public void validate(User record, int recordNum) {
      if (record == null) {
        throw new NullPointerException("Unexpected null record from Reader");
      }

      Preconditions.checkArgument(record.equals(data.get(recordNum)),
          "Record %s does not equal %s", record, data.get(recordNum));
    }
    
  }

  public static class User {
    private String name;
    private String color;

    public User() {
    }

    public User(String name, String color) {
      this.name = name;
      this.color = color;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public void setColor(String color) {
      this.color = color;
    }

    public String getColor() {
      return color;
    }

    @Override
    public String toString() {
      return String.format("User { name = '%s'; color = '%s'; }", name, color);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final User other = (User) obj;
      if ((this.name == null) ? (other.name != null) : !this.name.equals(other.name)) {
        return false;
      }
      if ((this.color == null) ? (other.color != null) : !this.color.equals(other.color)) {
        return false;
      }
      return true;
    }

    @Override
    public int hashCode() {
      int hash = 7;
      hash = 67 * hash + (this.name != null ? this.name.hashCode() : 0);
      hash = 67 * hash + (this.color != null ? this.color.hashCode() : 0);
      return hash;
    }
  }

}
