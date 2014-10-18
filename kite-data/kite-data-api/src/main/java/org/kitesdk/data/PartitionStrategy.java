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

import org.kitesdk.data.spi.PartitionStrategyBuilderFactory;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * The strategy used to determine how a dataset is partitioned.
 * </p>
 * <p>
 * When a {@link Dataset} is configured
 * with a partition strategy, that data is considered partitioned. Any entities
 * written to a partitioned dataset are evaluated with its
 * {@code PartitionStrategy} to determine which partition to write to.
 * </p>
 * <p>
 * You should use the inner {@link Builder} to create new instances.
 * </p>
 * 
 * @see DatasetDescriptor
 * @see Dataset
 */
@Immutable
@SuppressWarnings("deprecation")
public abstract class PartitionStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionStrategy.class);

  /**
   * No argument constructor for sub-classes.
   */
  protected PartitionStrategy() {
  }

  /**
   * <p>
   * Return the cardinality produced by the contained field partitioners.
   * </p>
   * <p>
   * This can be used to aid in calculating resource usage during certain
   * operations. For example, when writing data to a partitioned dataset, you
   * can use this method to estimate (or discover exactly, depending on the
   * partition functions) how many leaf partitions exist.
   * </p>
   * <p>
   * <strong>Warning:</strong> This method is allowed to lie and should be
   * treated only as a hint. Some partition functions are fixed (for example, 
   * hash modulo number of buckets), while others are open-ended (for
   * example, discrete value) and depend on the input data.
   * </p>
   * 
   * @return The estimated (or possibly concrete) number of leaf partitions.
   */
  public abstract int getCardinality();

  /**
   * Return a {@link PartitionStrategy} for subpartitions starting at the given
   * index.
   */
  protected abstract PartitionStrategy getSubpartitionStrategy(int startIndex);

  /**
   * @param pretty {@code true} to indent and format JSON
   * @return this PartitionStrategy as its JSON representation
   */
  public abstract String toString(boolean pretty);

  /**
   * A fluent builder to aid in the construction of {@link PartitionStrategy}s.
   */
  public static class Builder {

    private final Builder delegate;

    public Builder() {
      delegate = PARTITION_STRATEGY_BUILDER_FACTORY.newBuilder();
    }

    protected Builder(@Nullable Builder delegate) {
      this.delegate = delegate;
    }

    /**
     * Configure a hash partitioner with the specified number of
     * {@code buckets}.
     *
     * The partition name is the source field name with a "_hash" suffix.
     * For example, hash("color", 34) creates "color_hash" partitions.
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @param buckets
     *          The number of buckets into which data is to be partitioned.
     * @return An instance of the builder for method chaining.
     */
    public Builder hash(String sourceName, int buckets) {
      return delegate.hash(sourceName, buckets);
    }

    /**
     * Configure a hash partitioner with the specified number of
     * {@code buckets}. If name is null, the partition name will be the source
     * field name with a "_hash" suffix. For example, hash("color", null, 34)
     * will create "color_hash" partitions.
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @param name
     *          The entity field name of the partition.
     * @param buckets
     *          The number of buckets into which data is to be partitioned.
     * @return An instance of the builder for method chaining.
     * @since 0.3.0
     */
    public Builder hash(String sourceName, @Nullable String name, int buckets) {
      return delegate.hash(sourceName, name, buckets);
    }

    /**
     * Configure an identity partitioner.
     *
     * The partition name is the source field name with a "_copy" suffix.
     * For example, identity("color", String.class, 34) creates "color_copy"
     * partitions.
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @return An instance of the builder for method chaining.
     * @see IdentityFieldPartitioner
     * @since 0.14.0
     */
    @SuppressWarnings("unchecked")
    public Builder identity(String sourceName) {
      return delegate.identity(sourceName);
    }

    /**
     * Configure an identity partitioner. If name is null, the partition name
     * will be the source field name with a "_copy" suffix. For example,
     * identity("color", null, ...) will create "color_copy" partitions.
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @param name
     *          A name for the partition field
     * @return An instance of the builder for method chaining.
     * @since 0.14.0
     */
    @SuppressWarnings("unchecked")
    public Builder identity(String sourceName, String name) {
      return delegate.identity(sourceName, name);
    }

    /**
     * Configure an identity partitioner with a cardinality hint of
     * {@code cardinalityHint}.
     *
     * The partition name is the source field name with a "_copy" suffix.
     * For example, identity("color", String.class, 34) creates "color_copy"
     * partitions.
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @param cardinalityHint
     *          A hint as to the number of partitions that will be created (i.e.
     *          the number of discrete values for the field {@code name} in the
     *          data).
     * @return An instance of the builder for method chaining.
     * @since 0.14.0
     */
    @SuppressWarnings("unchecked")
    public Builder identity(String sourceName, int cardinalityHint) {
      return delegate.identity(sourceName, cardinalityHint);
    }

    /**
     * Configure an identity partitioner with a cardinality hint of
     * {@code cardinalityHint}. If name is null, the partition name will be the source
     * field name with a "_copy" suffix. For example, identity("color", null, ...)
     * will create "color_copy" partitions.
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @param name
     *          A name for the partition field
     * @param cardinalityHint
     *          A hint as to the number of partitions that will be created (i.e.
     *          the number of discrete values for the field {@code name} in the
     *          data).
     * @return An instance of the builder for method chaining.
     * @since 0.14.0
     */
    @SuppressWarnings("unchecked")
    public Builder identity(String sourceName, String name, int cardinalityHint) {
      return delegate.identity(sourceName, name, cardinalityHint);
    }

    /**
     * Configure a range partitioner with a set of {@code upperBounds}.
     *
     * The partition name will be the source field name with a "_bound" suffix.
     * For example, range("number", 5, 10) creates "number_bound"
     * partitions.
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @param upperBounds
     *          A variadic list of upper bounds of each partition.
     * @return An instance of the builder for method chaining.
     */
    public Builder range(String sourceName, int... upperBounds) {
      return delegate.range(sourceName, upperBounds);
    }

    /**
     * Configure a range partitioner for strings with a set of {@code upperBounds}.
     *
     * The partition name will be the source field name with a "_bound" suffix.
     * For example, range("color", "blue", "green") creates "color_bound"
     * partitions.
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @param upperBounds
     *          A variadic list of upper bounds of each partition.
     * @return An instance of the builder for method chaining.
     */
    public Builder range(String sourceName, String... upperBounds) {
      return delegate.range(sourceName, upperBounds);
    }

    /**
     * Configure a partitioner for extracting the year from a timestamp field.
     * The UTC timezone is assumed. If name is null, the partition entity name
     * will be "year".
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @param name
     *          The entity field name of the partition.
     * @return An instance of the builder for method chaining.
     * @since 0.3.0
     */
    public Builder year(String sourceName, @Nullable String name) {
      return delegate.year(sourceName, name);
    }

    /**
     * Configure a partitioner for extracting the year from a timestamp field.
     * The UTC timezone is assumed. The partition entity name is "year".
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @return An instance of the builder for method chaining.
     * @since 0.8.0
     */
    public Builder year(String sourceName) {
      return delegate.year(sourceName);
    }

    /**
     * Configure a partitioner for extracting the month from a timestamp field.
     * The UTC timezone is assumed. If name is null, the partition entity name
     * will be "month".
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @param name
     *          The entity field name of the partition.
     * @return An instance of the builder for method chaining.
     * @since 0.3.0
     */
    public Builder month(String sourceName, @Nullable String name) {
      return delegate.month(sourceName, name);
    }

    /**
     * Configure a partitioner for extracting the month from a timestamp field.
     * The UTC timezone is assumed. The partition entity name is "month".
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @return An instance of the builder for method chaining.
     * @since 0.8.0
     */
    public Builder month(String sourceName) {
      return delegate.month(sourceName);
    }

    /**
     * Configure a partitioner for extracting the day from a timestamp field.
     * The UTC timezone is assumed. If name is null, the partition entity name
     * will be "day".
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @param name
     *          The entity field name of the partition.
     * @return An instance of the builder for method chaining.
     * @since 0.3.0
     */
    public Builder day(String sourceName, @Nullable String name) {
      return delegate.day(sourceName, name);
    }

    /**
     * Configure a partitioner for extracting the day from a timestamp field.
     * The UTC timezone is assumed. The partition entity name is "day".
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @return An instance of the builder for method chaining.
     * @since 0.8.0
     */
    public Builder day(String sourceName) {
      return delegate.day(sourceName);
    }

    /**
     * Configure a partitioner for extracting the hour from a timestamp field.
     * The UTC timezone is assumed. If name is null, the partition entity name
     * will be "hour".
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @param name
     *          The entity field name of the partition.
     * @return An instance of the builder for method chaining.
     * @since 0.3.0
     */
    public Builder hour(String sourceName, @Nullable String name) {
      return delegate.hour(sourceName, name);
    }

    /**
     * Configure a partitioner for extracting the hour from a timestamp field.
     * The UTC timezone is assumed. The partition entity name is "hour".
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @return An instance of the builder for method chaining.
     * @since 0.8.0
     */
    public Builder hour(String sourceName) {
      return delegate.hour(sourceName);
    }

    /**
     * Configure a partitioner for extracting the minute from a timestamp field.
     * The UTC timezone is assumed. If name is null, the partition entity name
     * will be "minute".
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @param name
     *          The entity field name of the partition.
     * @return An instance of the builder for method chaining.
     * @since 0.3.0
     */
    public Builder minute(String sourceName, @Nullable String name) {
      return delegate.minute(sourceName, name);
    }

    /**
     * Configure a partitioner for extracting the minute from a timestamp field.
     * The UTC timezone is assumed. The partition entity name is "minute".
     *
     * @param sourceName
     *          The entity field name from which to get values to be
     *          partitioned.
     * @return An instance of the builder for method chaining.
     * @since 0.8.0
     */
    public Builder minute(String sourceName) {
      return delegate.minute(sourceName);
    }

    /**
     * Configure a partitioner that applies a custom date format to a timestamp
     * field. The UTC timezone is assumed.
     *
     * @param sourceName
     *          The entity field name of the timestamp to format
     * @param name
     *          A name for the partitions created by the format (e.g. "day")
     * @param format
     *          A {@link java.text.SimpleDateFormat} format-string.
     * @return This builder for method chaining.
     * @since 0.9.0
     */
    public Builder dateFormat(String sourceName, String name, String format) {
      return delegate.dateFormat(sourceName, name, format);
    }

    /**
     * Configure a partitioner that uses values always provided at runtime.
     * <p>
     * The partitioner created by this method will expect {@link String} values.
     *
     * @param name
     *          A name for the partitions
     * @return This builder for method chaining
     *
     * @since 0.17.0
     */
    public Builder provided(String name) {
      return delegate.provided(name);
    }

    /**
     * Configure a partitioner that uses values always provided at runtime.
     * <p>
     * The partitioner created by this method will expect values based on the
     * given {@code valuesType}: "string", "int", or "long".
     *
     * @param name
     *          A name for the partitions
     * @param valuesType
     *          A type string for values this partitioner will expect; one of
     *          "string", "int", or "long". If null, the default is "string".
     * @return This builder for method chaining
     *
     * @since 0.17.0
     */
    public Builder provided(String name, @Nullable String valuesType) {
      return delegate.provided(name, valuesType);
    }

    /**
     * Build a configured {@link PartitionStrategy} instance.
     *
     * This builder should be considered single use and discarded after a call
     * to this method.
     *
     * @return The configured instance of {@link PartitionStrategy}.
     * @since 0.9.0
     */
    public PartitionStrategy build() {
      return delegate.build();
    }
  }

  private static final PartitionStrategyBuilderFactory PARTITION_STRATEGY_BUILDER_FACTORY;


  static {
    ServiceLoader<PartitionStrategyBuilderFactory> factories =
        ServiceLoader.load(PartitionStrategyBuilderFactory.class);

    PartitionStrategyBuilderFactory selectedFactory = null;
    for (PartitionStrategyBuilderFactory factory : factories) {
      LOG.debug("Using {} for building PartitionStrategy implementations",
          factory.getClass());
      selectedFactory = factory;
      break;
    }

    if (selectedFactory == null) {
      String msg = "No implementation of " + PartitionStrategy.class
          + " available. Make sure that kite-data-common is on the classpath";
      LOG.error(msg);
      throw new RuntimeException(msg);
    }

    PARTITION_STRATEGY_BUILDER_FACTORY = selectedFactory;
  }

}
