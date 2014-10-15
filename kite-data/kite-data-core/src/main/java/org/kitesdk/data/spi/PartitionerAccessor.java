/*
 * Copyright 2014 Cloudera, Inc.
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

import java.util.List;
import org.kitesdk.data.PartitionStrategy;

/**
 * @since 0.18.0
 */
public interface PartitionerAccessor {

  /**
   * <p>
   * Get the list of field partitioners used for partitioning.
   * </p>
   * <p>
   * {@link FieldPartitioner}s are returned in the same order they are used
   * during partition selection.
   * </p>
   * @return list of {@link FieldPartitioner}s
   */
  public List<FieldPartitioner> getFieldPartitioners();

  /**
   * Get a partitioner by partition name.
   * 
   * @param name the name of the partition
   * @return a FieldPartitioner with the given partition name
   */
  public FieldPartitioner getPartitioner(String name);

  /**
   * Check if a partitioner for the partition name exists.
   * 
   * @param name the name of the partition
   * @return {@code true} if this strategy has a partitioner for the name
   */
  public boolean hasPartitioner(String name);

  /**
   * Return a {@link PartitionStrategy} for subpartitions starting at the given
   * index.
   * 
   * @param startIndex the start index
   * @return the PartitionStrategy for the subpartitions
   */
  public PartitionStrategy getSubpartitionStrategy(int startIndex);

}
