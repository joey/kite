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

import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.spi.AbstractRefinableView;
import org.kitesdk.data.spi.Constraints;


public class KafkaView<E> extends AbstractRefinableView<E> {

  private final String zkConnection;

  public KafkaView(String zkConnection, Dataset<E> dataset, Class<E> type) {
    super(dataset, type);
    this.zkConnection = zkConnection;
  }

  public KafkaView(String zkConnection, AbstractRefinableView<E> view, Constraints constraints) {
    super(view, constraints);
    this.zkConnection = zkConnection;
  }

  @Override
  protected AbstractRefinableView<E> filter(Constraints c) {
    return new KafkaView<E>(zkConnection, this, c);
  }

  @Override
  public DatasetReader<E> newReader() {
    KafkaAvroReader<E> reader = new KafkaAvroReader<E>(this);
    reader.initialize();
    return reader;
  }

  @Override
  public DatasetWriter<E> newWriter() {
    KafkaAvroWriter<E> writer = new KafkaAvroWriter<E>(this);
    writer.initialize();
    return writer;
  }

  String getZkConnection() {
    return zkConnection;
  }

}
