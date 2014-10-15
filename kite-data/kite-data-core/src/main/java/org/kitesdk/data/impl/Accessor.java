/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.kitesdk.data.impl;

import java.util.List;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.spi.PartitionerAccessor;

public class Accessor {

  /**
   * <p>
   * Get the list of field partitioners used for partitioning.
   * </p>
   * <p>
   * {@link FieldPartitioner}s are returned in the same order they are used
   * during partition selection.
   * </p>
   *
   * @param strategy the strategy to get partitioners from
   * @return list of {@link FieldPartitioner}s
   */
  public static List<FieldPartitioner> getFieldPartitioners(
      PartitionStrategy strategy) {

    return getAccessor(strategy).getFieldPartitioners();
  }

  /**
   * Get a partitioner by partition name.
   *
   * @param strategy the strategy to get the partitioner from
   * @param name the name of the partition
   * @return a FieldPartitioner with the given partition name
   */
  public static FieldPartitioner getPartitioner(PartitionStrategy strategy,
      String name) {
    return getAccessor(strategy).getPartitioner(name);
  }

  /**
   * Check if a partitioner for the partition name exists.
   *
   * @param strategy the strategy to check
   * @param name the name of the partition
   * @return {@code true} if this strategy has a partitioner for the name
   */
  public static boolean hasPartitioner(PartitionStrategy strategy, String name) {
    return getAccessor(strategy).hasPartitioner(name);
  }

  /**
   * Return a {@link PartitionStrategy} for subpartitions starting at the given
   * index.
   *
   * @param strategy the strategy to get subpartitions from
   * @param startIndex the start index
   * @return the PartitionStrategy for the subpartitions
   */
  public static PartitionStrategy getSubpartitionStrategy(
      PartitionStrategy strategy, int startIndex) {
    return getAccessor(strategy).getSubpartitionStrategy(startIndex);
  }

  public static String toExpression(PartitionStrategy partitionStrategy) {
    return PartitionExpression.toExpression(partitionStrategy);
  }

  public static PartitionStrategy fromExpression(String partitionExpression) {
    return new PartitionExpression(partitionExpression, true).evaluate();
  }

  private static PartitionerAccessor getAccessor(PartitionStrategy strategy) {
    PartitionerAccessor accessor = null;
    if (strategy instanceof PartitionerAccessor) {
      accessor = (PartitionerAccessor) strategy;
    } else {
      throw new ClassCastException("PartitionStrategy implementation "
          + strategy.getClass() + " does not implement PartitionerAccessor. "
          + "Make sure kite-data-common is on the classpath before other "
          + "jars.");
    }

    return accessor;
  }

}
