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

package org.kitesdk.data.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.spi.PartitionStrategyParser;


public class KafkaPartitioner implements Partitioner { 

  public KafkaPartitioner(VerifiableProperties properties) {
  }

  @Override
  public int partition(Object o, int i) {
    return 0;
  }


}
