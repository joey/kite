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
package org.kitesdk.data.spi;

import java.net.URI;
import java.net.URL;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.kitesdk.data.ColumnMapping;
import org.kitesdk.data.CompressionType;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Format;
import org.kitesdk.data.PartitionStrategy;


public interface DatasetDescriptorFactory {

  public DatasetDescriptor newDatasetDescriptor(Schema schema, @Nullable URL schemaUrl, Format format, @Nullable URI location, @Nullable Map<String, String> properties, @Nullable PartitionStrategy partitionStrategy);

  public DatasetDescriptor newDatasetDescriptor(Schema schema, @Nullable URL schemaUrl, Format format, @Nullable URI location, @Nullable Map<String, String> properties, @Nullable PartitionStrategy partitionStrategy, @Nullable ColumnMapping columnMapping);

  public DatasetDescriptor newDatasetDescriptor(Schema schema, @Nullable URI schemaUri, Format format, @Nullable URI location, @Nullable Map<String, String> properties, @Nullable PartitionStrategy partitionStrategy, @Nullable ColumnMapping columnMapping, @Nullable CompressionType compressionType);
  
}
