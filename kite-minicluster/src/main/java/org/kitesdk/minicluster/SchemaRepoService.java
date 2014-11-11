/**
 * Copyright 2014 Cloudera, Inc.
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
package org.kitesdk.minicluster;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.schemarepo.InMemoryRepository;
import org.schemarepo.server.RepositoryServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Schema Repository MiniCluster service implementation.
 */
public class SchemaRepoService implements Service {

  private static final Logger logger = LoggerFactory
      .getLogger(SchemaRepoService.class);

  static {
    MiniCluster.registerService(SchemaRepoService.class);
  }

  /**
   * Configuration settings
   */
  private Configuration hadoopConf;
  private String workDir;
  private String bindIP = "127.0.0.1";
  private String port = null;

  /**
   * Embedded Schema Repository
   */
  private RepositoryServer server;
  private boolean started = false;

  public SchemaRepoService() {
  }

  @Override
  public void configure(ServiceConfig serviceConfig) {
    this.workDir = serviceConfig.get(MiniCluster.WORK_DIR_KEY);
    if (serviceConfig.contains(MiniCluster.BIND_IP_KEY)) {
      bindIP = serviceConfig.get(MiniCluster.BIND_IP_KEY);
    }
    if (serviceConfig.contains(MiniCluster.SCHEMA_REPO_PORT_KEY)) {
      port = serviceConfig.get(MiniCluster.SCHEMA_REPO_PORT_KEY);
    }
    
    hadoopConf = serviceConfig.getHadoopConf();

    if (hadoopConf == null) {
      hadoopConf = new Configuration();
    }
  }

  @Override
  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  @Override
  public void start() throws IOException, InterruptedException {
    Preconditions.checkState(workDir != null,
        "The localBaseFsLocation must be set before starting cluster.");

    stop();

    String host = bindIP.equals("0.0.0.0") ? "localhost" : bindIP;
    if (port == null) {
      port = getAvailablePort();
    }

    Properties props = new Properties();
    props.setProperty("schema-repo.class", InMemoryRepository.class.getName());
    props.setProperty("jetty.host", host);
    props.setProperty("jetty.port", port);

    server = new RepositoryServer(props);
    try {
      server.start();
    } catch (Exception ex) {
      logger.error("Schema repository server failed to start on {}:{} with error {}",
          new Object[] {host, port, ex.getMessage()});
      logger.debug("Stack trace follows", ex);
      throw new RuntimeException(ex);
    }

    started = true;

    logger.info("Schema repository service started on {}:{}", host, port);
  }

  @Override
  public void stop() throws IOException {
    if (!started) {
      return;
    }

    try {
      server.stop();
    } catch (Exception ex) {
      logger.error("Schema repository server failed to stop with error {}",
          ex.getMessage());
      logger.debug("Stacktrace follows", ex);
      throw new RuntimeException(ex);
    } finally {
      started = false;
      server = null;
    }

    logger.info("Schema repository service shut down.");
  }

  @Override
  public List<Class<? extends Service>> dependencies() {
    return new ArrayList<Class<? extends Service>>();
  }

  private static String getAvailablePort() throws IOException {
    ServerSocket socket = new ServerSocket(0);
    try {
      return Integer.toString(socket.getLocalPort());
    } finally {
      socket.close();
    }
  }
}
