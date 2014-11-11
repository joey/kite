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
import java.util.Map;
import org.kitesdk.data.URIBuilder;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.spi.Loadable;
import org.kitesdk.data.spi.OptionBuilder;
import org.kitesdk.data.spi.Registration;
import org.kitesdk.data.spi.URIPattern;

public class Loader implements Loadable {

  @Override
  public void load() {
    final URIPattern repoPattern = new URIPattern("kafka::zk");
    OptionBuilder<DatasetRepository> builder = new OptionBuilder<DatasetRepository>() {
      @Override
      public DatasetRepository getFromOptions(Map<String, String> options) {
        return new KafkaDatasetRepository(options.get("zk"), options.get("schemaRepo"),
            URI.create("repo:" + repoPattern.construct(options)));
      }
    };

    Registration.register(repoPattern,
        new URIPattern("kafka::zk/:namespace/:dataset"),
        builder);
    
    Registration.register(repoPattern,
        new URIPattern("kafka::zk/:dataset?namespace="+URIBuilder.NAMESPACE_DEFAULT),
        builder);
  }
}
