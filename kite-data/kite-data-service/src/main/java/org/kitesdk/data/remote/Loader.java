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

package org.kitesdk.data.remote;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import org.kitesdk.data.spi.DatasetRepository;
import org.kitesdk.data.DatasetRepositoryException;
import org.kitesdk.data.spi.Loadable;
import org.kitesdk.data.spi.OptionBuilder;
import org.kitesdk.data.spi.Pair;
import org.kitesdk.data.spi.Registration;
import org.kitesdk.data.spi.URIBuilder;
import org.kitesdk.data.spi.URIPattern;

/**
 * A Loader implementation to register URIs for RemoteDatasetRepositories.
 */
public class Loader implements Loadable {

  /**
   * This class builds configured instances of
   * {@code RemoteDatasetRepository} from a Map of options. This is for the
   * URI system.
   */
  private static class RemoteOptionBuilder implements OptionBuilder<DatasetRepository> {

    @Override
    public DatasetRepository getFromOptions(Map<String, String> match) {
      String hostname = match.get(URIPattern.HOST);
      int port = Integer.parseInt(match.get(URIPattern.PORT));
      String remoteUri = match.get("uri");

      // Parse the dataset name from the remote URI
      if (remoteUri.startsWith(URIBuilder.DATASET_SCHEME)) {
        URI uri = URI.create(remoteUri);
        Map<String, String> remoteOptions = Registration.lookupDatasetPattern(
            URI.create(uri.getRawSchemeSpecificPart())).second();

        // Add the dataset name to our options
        match.put(URIBuilder.DATASET_NAME_OPTION,
            remoteOptions.get(URIBuilder.DATASET_NAME_OPTION));
      } 

      try {
        return new RemoteDatasetRepository(hostname, port, remoteUri);
      } catch (IOException ex) {
        throw new DatasetRepositoryException(ex);
      }
    }
  }

  @Override
  public void load() {
    // get a default Configuration to configure defaults (so it's okay!)
    OptionBuilder<DatasetRepository> builder =
        new RemoteOptionBuilder();

    Registration.register(
        new URIPattern("remote:/*uri"),
        new URIPattern("remote:/*uri"),
        builder);
  }
}
