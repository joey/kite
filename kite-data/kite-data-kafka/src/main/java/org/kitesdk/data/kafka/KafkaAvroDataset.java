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

import java.net.URI;
import org.apache.avro.Schema;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.RefinableView;
import org.kitesdk.data.URIBuilder;
import org.kitesdk.data.spi.AbstractDataset;
import org.kitesdk.data.spi.AbstractRefinableView;
import org.kitesdk.data.spi.Constraints;


public class KafkaAvroDataset<E> extends AbstractDataset<E> {

  private final String zkConnection;
  private final String namespace;
  private final String name;
  private final DatasetDescriptor descriptor;
  private final URI uri;

  KafkaAvroDataset(String zkConnection, String namespace, String name, Class<E> type,
      Schema schema, DatasetDescriptor descriptor, URI repositoryUri) {
    super(type, schema);

    this.zkConnection = zkConnection;
    this.namespace = namespace;
    this.name = name;
    this.descriptor = descriptor;
    this.uri = URIBuilder.build(repositoryUri, namespace, name);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getNamespace() {
    return namespace;
  }

  @Override
  public DatasetDescriptor getDescriptor() {
    return descriptor;
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public boolean isEmpty() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  protected RefinableView<E> asRefinableView() {
    return new KafkaView<E>(zkConnection, this, type);
  }

  @Override
  public AbstractRefinableView<E> filter(Constraints c) {
    return new KafkaView<E>(zkConnection, this, type).filter(c);
  }
}
