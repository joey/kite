/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data;

import org.kitesdk.data.spi.DatasetDescriptorBuilderFactory;
import org.kitesdk.data.spi.DatasetDescriptorFactory;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.Map;
import java.util.ServiceLoader;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * The structural definition of a {@link Dataset}.
 * </p>
 * <p>
 * Each {@code Dataset} has an associated {@link Schema} and optional
 * {@link PartitionStrategy} defined at the time of creation. You use instances
 * of this class to hold this information. You are strongly encouraged to use
 * the inner {@link Builder} to create new instances.
 * </p>
 */
@Immutable
public class DatasetDescriptor {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetDescriptor.class);

  private final DatasetDescriptor delegate;

  /**
   * Create an instance of this class with the supplied {@link Schema},
   * optional URL, {@link Format}, optional location URL, and optional
   * {@link PartitionStrategy}.
   */
  public DatasetDescriptor(Schema schema, @Nullable URL schemaUrl, Format format,
      @Nullable URI location, @Nullable Map<String, String> properties,
      @Nullable PartitionStrategy partitionStrategy) {
    delegate = DATASET_DESCRIPTOR_FACTORY.newDatasetDescriptor(schema, schemaUrl,
        format, location, properties, partitionStrategy);
  }

  /**
   * Create an instance of this class with the supplied {@link Schema}, optional
   * URL, {@link Format}, optional location URL, optional
   * {@link PartitionStrategy}, and optional {@link ColumnMapping}.
   *
   * @since 0.14.0
   */
  public DatasetDescriptor(Schema schema, @Nullable URL schemaUrl,
      Format format, @Nullable URI location,
      @Nullable Map<String, String> properties,
      @Nullable PartitionStrategy partitionStrategy,
      @Nullable ColumnMapping columnMapping) {
    delegate = DATASET_DESCRIPTOR_FACTORY.newDatasetDescriptor(schema, schemaUrl,
        format, location, properties, partitionStrategy, columnMapping);
  }

  /**
   * Create an instance of this class with the supplied {@link Schema}, optional
   * URL, {@link Format}, optional location URL, optional
   * {@link PartitionStrategy}, optional {@link ColumnMapping}, and optional
   * {@link CompressionType}.
   *
   * @since 0.17.0
   */
  public DatasetDescriptor(Schema schema, @Nullable URI schemaUri,
      Format format, @Nullable URI location,
      @Nullable Map<String, String> properties,
      @Nullable PartitionStrategy partitionStrategy,
      @Nullable ColumnMapping columnMapping,
      @Nullable CompressionType compressionType) {
    delegate = DATASET_DESCRIPTOR_FACTORY.newDatasetDescriptor(schema, schemaUri,
        format, location, properties, partitionStrategy, columnMapping,
        compressionType);
  }

  /**
   * No argument constructor for sub-classes
   */
  protected DatasetDescriptor() {
    delegate = null;
  }

  /**
   * Get the associated {@link Schema}. Depending on the underlying storage
   * system, this schema can be simple (that is, records made up of only scalar
   * types) or complex (that is, containing other records, lists, and so on).
   * Validation of the supported schemas is performed by the managing
   * repository, not the dataset or descriptor itself.
   *
   * @return the schema
   */
  public Schema getSchema() {
    return delegate.getSchema();
  }

  /**
   * Get a URL from which the {@link Schema} can be retrieved (optional). This
   * method might return {@code null} if the schema is not stored at a persistent
   * URL (for example, if it were constructed from a literal string).
   *
   * @return a URL from which the schema can be retrieved
   * @since 0.3.0
   */
  @Nullable
  public URL getSchemaUrl() {
    return delegate.getSchemaUrl();
  }

  /**
   * Get the associated {@link Format} the data is stored in.
   *
   * @return the format
   * @since 0.2.0
   */
  public Format getFormat() {
    return delegate.getFormat();
  }

  /**
   * Get the URL location where the data for this {@link Dataset} is stored
   * (optional).
   *
   * @return a location URL or null if one is not set
   *
   * @since 0.8.0
   */
  @Nullable
  public URI getLocation() {
    return delegate.getLocation();
  }

  /**
   * Get a named property.
   *
   * @param name the String property name to get.
   * @return the String value of the property, or null if it does not exist.
   *
   * @since 0.8.0
   */
  @Nullable
  public String getProperty(String name) {
    return delegate.getProperty(name);
  }

  /**
   * Check if a named property exists.
   *
   * @param name the String property name.
   * @return true if the property exists, false otherwise.
   *
   * @since 0.8.0
   */
  public boolean hasProperty(String name) {
    return delegate.hasProperty(name);
  }

  /**
   * List the names of all custom properties set.
   *
   * @return a Collection of String property names.
   *
   * @since 0.8.0
   */
  public Collection<String> listProperties() {
    return delegate.listProperties();
  }

  /**
   * Get the {@link PartitionStrategy}, if this dataset is partitioned. Calling
   * this method on a non-partitioned dataset is an error. Instead, use the
   * {@link #isPartitioned()} method prior to invocation.
   */
  public PartitionStrategy getPartitionStrategy() {
    return delegate.getPartitionStrategy();
  }

  /**
   * Get the {@link ColumnMapping}.
   *
   * @return ColumnMapping
   *
   * @since 0.14.0
   */
  public ColumnMapping getColumnMapping() {
    return delegate.getColumnMapping();
  }

  /**
   * Get the {@link CompressionType}
   *
   * @return the compression format
   *
   * @since 0.17.0
   */
  public CompressionType getCompressionType() {
    return delegate.getCompressionType();
  }

  /**
   * Returns true if an associated dataset is partitioned (that is, has an
   * associated {@link PartitionStrategy}), false otherwise.
   */
  public boolean isPartitioned() {
    return delegate.isPartitioned();
  }

  /**
   * Returns true if an associated dataset is column mapped (that is, has an
   * associated {@link ColumnMapping}), false otherwise.
   *
   * @since 0.14.0
   */
  public boolean isColumnMapped() {
    return delegate.isColumnMapped();
  }

  protected Map<String, String> getProperties() {
    return delegate.getProperties();
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final DatasetDescriptor other = (DatasetDescriptor) obj;
    return (delegate.equals(other.delegate));
  }

  @Override
  public int hashCode() {
    return delegate.hashCode();
  }

  /**
   * A fluent builder to aid in the construction of {@link DatasetDescriptor}s.
   */
  public static class Builder {

    private final Builder delegate;

    public Builder() {
      delegate = DATASET_DESRIPTOR_BUILDER_FACTORY.newBuilder();
    }

    /**
     * Creates a Builder configured to copy {@code descriptor}, if it is not
     * modified. This is intended to help callers copy and update descriptors
     * even though they are {@link Immutable}.
     *
     * @param descriptor A {@link DatasetDescriptor} to copy settings from
     * @since 0.7.0
     */
    public Builder(DatasetDescriptor descriptor) {
      delegate = DATASET_DESRIPTOR_BUILDER_FACTORY.newBuilder(descriptor);
    }

    /**
     * Provided for sub-classes to skip delegate lookup
     *
     * @param delegate
     */
    protected Builder(@Nullable Builder delegate) {
      this.delegate = delegate;
    }

    /**
     * Configure the dataset's schema. A schema is required, and can be set
     * using one of the methods: {@code schema}, {@code schemaLiteral},
     * {@code schemaUri}, or {@code schemaFromAvroDataFile}.
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder schema(Schema schema) {
      return delegate.schema(schema);
    }

    /**
     * Configure the dataset's schema from a {@link File}. A schema is required,
     * and can be set using one of the methods {@code schema},
     * {@code schemaLiteral}, {@code schemaUri}, or
     * {@code schemaFromAvroDataFile}.
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder schema(File file) throws IOException {
      return delegate.schema(file);
    }

    /**
     * Configure the dataset's schema from an {@link InputStream}. It is the
     * caller's responsibility to close the {@link InputStream}. A schema is
     * required, and can be set using one of the methods {@code schema},
     * {@code schemaLiteral}, {@code schemaUri}, or
     * {@code schemaFromAvroDataFile}.
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder schema(InputStream in) throws IOException {
      return delegate.schema(in);
    }

    /**
     * Configure the {@link Dataset}'s schema from a URI. A schema is required,
     * and can be set using one of the methods {@code schema},
     * {@code schemaLiteral}, {@code schemaUri}, or
     * {@code schemaFromAvroDataFile}.
     *
     * @param uri a URI object for the schema's location.
     * @return An instance of the builder for method chaining.
     * @throws IOException
     *
     * @since 0.8.0
     */
    public Builder schemaUri(URI uri) throws IOException {
      return delegate.schemaUri(uri);
    }

    /**
     * Configure the {@link Dataset}'s schema from a String URI. A schema is
     * required, and can be set using one of the methods {@code schema},
     * {@code schemaLiteral}, {@code schemaUri}, or
     * {@code schemaFromAvroDataFile}.
     *
     * @param uri a String URI
     * @return An instance of the builder for method chaining.
     * @throws IOException
     *
     * @since 0.8.0
     */
    public Builder schemaUri(String uri) throws IOException {
      return delegate.schemaUri(uri);
    }

    /**
     * Configure the dataset's schema from a {@link String}. A schema is
     * required, and can be set using one of the methods {@code schema},
     * {@code schemaLiteral}, {@code schemaUri}, or
     * {@code schemaFromAvroDataFile}.
     *
     * @return An instance of the builder for method chaining.
     *
     * @since 0.8.0
     */
    public Builder schemaLiteral(String s) {
      return delegate.schemaLiteral(s);
    }

    /**
     * Configure the dataset's schema via a Java class type. A schema is
     * required, and can be set using one of the methods {@code schema},
     * {@code schemaLiteral}, {@code schemaUri}, or
     * {@code schemaFromAvroDataFile}.
     *
     * @return An instance of the builder for method chaining.
     * @since 0.2.0
     */
    public <T> Builder schema(Class<T> type) {
      return delegate.schema(type);
    }

    /**
     * Configure the dataset's schema by using the schema from an existing Avro
     * data file. A schema is required, and can be set using one of the methods
     * {@code schema}, {@code schemaLiteral}, {@code schemaUri}, or
     * {@code schemaFromAvroDataFile}.
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder schemaFromAvroDataFile(File file) throws IOException {
      return delegate.schemaFromAvroDataFile(file);
    }

    /**
     * Configure the dataset's schema by using the schema from an existing Avro
     * data file. It is the caller's responsibility to close the
     * {@link InputStream}. A schema is required, and can be set using one of
     * the methods  {@code schema},  {@code schemaLiteral}, {@code schemaUri},
     * or {@code schemaFromAvroDataFile}.
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder schemaFromAvroDataFile(InputStream in) throws IOException {
      return delegate.schemaFromAvroDataFile(in);
    }

    /**
     * Configure the dataset's schema by using the schema from an existing Avro
     * data file. A schema is required, and can be set using one of the methods
     * {@code schema}, {@code schemaLiteral}, {@code schemaUri}, or
     * {@code schemaFromAvroDataFile}.
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder schemaFromAvroDataFile(URI uri) throws IOException {
      return delegate.schemaFromAvroDataFile(uri);
    }

    /**
     * Configure the dataset's format (optional). If not specified
     * {@link Formats#AVRO} is used by default.
     *
     * @return An instance of the builder for method chaining.
     * @since 0.2.0
     */
    public Builder format(Format format) {
      return delegate.format(format);
    }

    /**
     * Configure the dataset's format from a format name String (optional). If
     * not specified, {@link Formats#AVRO} is used by default.
     *
     * @param formatName a String format name
     * @return An instance of the builder for method chaining.
     * @throws UnknownFormatException if the format name is not recognized.
     *
     * @since 0.8.0
     */
    public Builder format(String formatName) {
      return delegate.format(formatName);
    }

    /**
     * Configure the dataset's location (optional).
     *
     * @param uri A URI location
     * @return An instance of the builder for method chaining.
     *
     * @since 0.8.0
     */
    public Builder location(@Nullable URI uri) {
      return delegate.location(uri);
    }

    /**
     * Configure the dataset's location (optional).
     *
     * @param uri A location Path
     * @return An instance of the builder for method chaining.
     *
     * @since 0.8.0
     */
    /*

    TODO: Need to add Hadoop as a dependency for Path
    public Builder location(Path uri) {
      return delegate.location(uri);
    }
    */

    /**
     * Configure the dataset's location (optional).
     *
     * @param uri A location String URI
     * @return An instance of the builder for method chaining.
     *
     * @since 0.8.0
     */
    public Builder location(String uri) {
      return delegate.location(uri);
    }

    /**
     * Add a key-value property to the descriptor.
     *
     * @param name the property name
     * @param value the property value
     * @return An instance of the builder for method chaining.
     *
     * @since 0.8.0
     */
    public Builder property(String name, String value) {
      return delegate.property(name, value);
    }

    /**
     * Configure the dataset's partitioning strategy (optional).
     *
     * @return An instance of the builder for method chaining.
     */
    public Builder partitionStrategy(
        @Nullable PartitionStrategy partitionStrategy) {
      return delegate.partitionStrategy(partitionStrategy);
    }

    /**
     * Configure the dataset's partition strategy from a File.
     *
     * The File contents must be a JSON-formatted partition strategy that is
     * produced by {@link PartitionStrategy#toString()}.
     *
     * @param file
     *          The File
     * @return
     *          An instance of the builder for method chaining.
     * @throws ValidationException
     *          If the file does not contain a valid JSON-encoded partition
     *          strategy
     * @throws DatasetIOException
     *          If there is an IOException accessing the file contents
     *
     * @since 0.14.0
     */
    public Builder partitionStrategy(File file) {
      return delegate.partitionStrategy(file);
    }

    /**
     * Configure the dataset's partition strategy from an InputStream.
     *
     * The InputStream contents must be a JSON-formatted partition strategy
     * that is produced by {@link PartitionStrategy#toString()}.
     *
     * @param in
     *          The input stream
     * @return An instance of the builder for method chaining.
     * @throws ValidationException
     *          If the stream does not contain a valid JSON-encoded partition
     *          strategy
     * @throws DatasetIOException
     *          If there is an IOException accessing the InputStream contents
     *
     * @since 0.14.0
     */
    public Builder partitionStrategy(InputStream in) {
      return delegate.partitionStrategy(in);
    }

    /**
     * Configure the dataset's partition strategy from a String literal.
     *
     * The String literal is a JSON-formatted partition strategy that can be
     * produced by {@link PartitionStrategy#toString()}.
     *
     * @param literal
     *          A partition strategy String literal
     * @return This builder for method chaining.
     * @throws ValidationException
     *          If the literal is not a valid JSON-encoded partition strategy
     *
     * @since 0.14.0
     */
    public Builder partitionStrategyLiteral(String literal) {
      return delegate.partitionStrategyLiteral(literal);
    }

    /**
     * Configure the dataset's partition strategy from a URI.
     *
     * @param uri
     *          A URI to a partition strategy JSON file.
     * @return This builder for method chaining.
     * @throws ValidationException
     *          If the literal is not a valid JSON-encoded partition strategy
     *
     * @since 0.14.0
     */
    public Builder partitionStrategyUri(URI uri) throws IOException {
      return delegate.partitionStrategyUri(uri);
    }

    /**
     * Configure the dataset's partition strategy from a String URI.
     *
     * @param uri
     *          A String URI to a partition strategy JSON file.
     * @return This builder for method chaining.
     * @throws ValidationException
     *          If the literal is not a valid JSON-encoded partition strategy
     *
     * @since 0.14.0
     */
    public Builder partitionStrategyUri(String uri) throws IOException {
      return delegate.partitionStrategyUri(uri);
    }

    /**
     * Configure the dataset's column mapping descriptor (optional)
     *
     * @param columnMappings
     *          A ColumnMapping
     * @return This builder for method chaining
     *
     * @since 0.14.0
     */
    public Builder columnMapping(
        @Nullable ColumnMapping columnMappings) {
      return delegate.columnMapping(columnMappings);
    }

    /**
     * Configure the dataset's column mapping descriptor from a File.
     *
     * The File contents must be a JSON-formatted column mapping. This format
     * can produced by {@link ColumnMapping#toString()}.
     *
     * @param file
     *          The file
     * @return This builder for method chaining
     * @throws ValidationException
     *          If the literal is not valid JSON-encoded column mappings
     * @throws DatasetIOException
     *          If there is an IOException accessing the file contents
     *
     * @since 0.14.0
     */
    public Builder columnMapping(File file) {
      return delegate.columnMapping(file);
    }

    /**
     * Configure the dataset's column mapping descriptor from an InputStream.
     *
     * The InputStream contents must be a JSON-formatted column mapping. This
     * format can produced by {@link ColumnMapping#toString()}.
     *
     * @param in
     *          The input stream
     * @return This builder for method chaining
     * @throws ValidationException
     *          If the literal is not valid JSON-encoded column mappings
     * @throws DatasetIOException
     *          If there is an IOException accessing the InputStream contents
     *
     * @since 0.14.0
     */
    public Builder columnMapping(InputStream in) {
      return delegate.columnMapping(in);
    }

    /**
     * Configure the dataset's column mappings from a String literal.
     *
     * The String literal is a JSON-formatted representation that can be
     * produced by {@link ColumnMapping#toString()}.
     *
     * @param literal
     *          A column mapping String literal
     * @return This builder for method chaining
     * @throws ValidationException
     *          If the literal is not valid JSON-encoded column mappings
     *
     * @since 0.14.0
     */
    public Builder columnMappingLiteral(String literal) {
      return delegate.columnMappingLiteral(literal);
    }

    /**
     * Configure the dataset's column mappings from a URI.
     *
     * @param uri
     *          A URI to a column mapping JSON file
     * @return This builder for method chaining
     * @throws ValidationException
     *          If the literal is not valid JSON-encoded column mappings
     * @throws java.io.IOException
     *          If accessing the URI results in an IOException
     *
     * @since 0.14.0
     */
    public Builder columnMappingUri(URI uri) throws IOException {
      return delegate.columnMappingUri(uri);
    }

    /**
     * Configure the dataset's column mappings from a String URI.
     *
     * @param uri
     *          A String URI to a column mapping JSON file
     * @return This builder for method chaining
     * @throws ValidationException
     *          If the literal is not valid JSON-encoded column mappings
     * @throws java.io.IOException
     *          If accessing the URI results in an IOException
     *
     * @since 0.14.0
     */
    public Builder columnMappingUri(String uri) throws IOException {
      return delegate.columnMappingUri(uri);
    }

    /**
     * Configure the dataset's compression format (optional). If not set,
     * default to {@link CompressionType#Snappy}.
     *
     * @param compressionType the compression format
     *
     * @return This builder for method chaining
     *
     * @since 0.17.0
     */
    public Builder compressionType(CompressionType compressionType) {
      return delegate.compressionType(compressionType);
    }

    /**
     * Configure the dataset's compression format (optional). If not set,
     * default to {@link CompressionType#Snappy}.
     *
     * @param compressionTypeName  the name of the compression format
     *
     * @return This builder for method chaining
     *
     * @since 0.17.0
     */
    public Builder compressionType(String compressionTypeName) {
      return delegate.compressionType(compressionTypeName);
    }

    /**
     * Build an instance of the configured dataset descriptor. Subsequent calls
     * produce new instances that are similarly configured.
     *
     * @since 0.9.0
     */
    public DatasetDescriptor build() {
      return delegate.build();
    }

    protected static Map<String, String> getProperties(DatasetDescriptor descriptor) {
      return descriptor.getProperties();
    }

  }

  private static final DatasetDescriptorFactory DATASET_DESCRIPTOR_FACTORY;


  private static final DatasetDescriptorBuilderFactory DATASET_DESRIPTOR_BUILDER_FACTORY;


  static {
    ServiceLoader<DatasetDescriptorFactory> descriptorFactories =
        ServiceLoader.load(DatasetDescriptorFactory.class);

    DatasetDescriptorFactory selectedDescriptorFactory = null;
    for (DatasetDescriptorFactory factory : descriptorFactories) {
      LOG.debug("Using {} to build DatasetDescriptor objects", factory.getClass());
      selectedDescriptorFactory = factory;
      break;
    }

    if (selectedDescriptorFactory == null) {
      String msg = "No implementation of org.kitesdk.data.DatasetDescriptor "
          + "available. Make sure that kite-data-common is on the classpath";
      LOG.error(msg);
      throw new RuntimeException(msg);
    }

    DATASET_DESCRIPTOR_FACTORY = selectedDescriptorFactory;

    ServiceLoader<DatasetDescriptorBuilderFactory> builderFactories =
        ServiceLoader.load(DatasetDescriptorBuilderFactory.class);

    DatasetDescriptorBuilderFactory selectedBuilderFactory = null;
    for (DatasetDescriptorBuilderFactory factory : builderFactories) {
      LOG.debug("Using {} to build DatasetDescriptor.Builder objects", factory.getClass());
      selectedBuilderFactory = factory;
      break;
    }

    if (selectedBuilderFactory == null) {
      String msg = "No implementation of org.kitesdk.data.DatasetDescriptor "
          + "available. Make sure that kite-data-common is on the classpath";
      LOG.error(msg);
      throw new RuntimeException(msg);
    }

    DATASET_DESRIPTOR_BUILDER_FACTORY = selectedBuilderFactory;
  }

}
