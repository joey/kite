/**
 * Copyright 2014 LinkedIn Corporation
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

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Preconditions;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Kafka minicluster service implementation.
 *
 * This class uses code from KafkaCluster from the Camus tests.
 */
public class KafkaService implements Service {

  private static final Logger logger = LoggerFactory
      .getLogger(KafkaService.class);

  static {
    MiniCluster.registerService(KafkaService.class);
  }

  /**
   * Configuration settings
   */
  private Configuration hadoopConf;
  private String workDir;
  private Integer zkClientPort = 2828;
  private String bindIP = "127.0.0.1";
  private Boolean clean = false;
  private int numKafkaBrokers = 2;

  /**
   * Embedded Kafka cluster
   */
  private List<KafkaServer> brokers;
  private boolean started = false;

  public KafkaService() {
  }

  @Override
  public void configure(ServiceConfig serviceConfig) {
    this.workDir = serviceConfig.get(MiniCluster.WORK_DIR_KEY);
    if (serviceConfig.contains(MiniCluster.BIND_IP_KEY)) {
      bindIP = serviceConfig.get(MiniCluster.BIND_IP_KEY);
    }
    if (serviceConfig.contains(MiniCluster.CLEAN_KEY)) {
      clean = Boolean.parseBoolean(serviceConfig.get(MiniCluster.CLEAN_KEY));
    }
    if (serviceConfig.contains(MiniCluster.ZK_PORT_KEY)) {
      zkClientPort = Integer.parseInt(serviceConfig.get(MiniCluster.ZK_PORT_KEY));
    }
    if (serviceConfig.contains(MiniCluster.NUM_KAFKA_BROKERS)) {
      numKafkaBrokers = Integer.parseInt(serviceConfig.get(MiniCluster.NUM_KAFKA_BROKERS));
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

  // from com.linkedin.camus.etl.kafka.KafkaCluster
  @Override
  public void start() throws IOException, InterruptedException {
    Preconditions.checkState(workDir != null,
        "The localBaseFsLocation must be set before starting cluster.");

    stop();

    brokers = Lists.newArrayList(numKafkaBrokers);

    String host = bindIP.equals("0.0.0.0") ? "localhost" : bindIP;

    for (int id = 1; id <= numKafkaBrokers; id++) {
      int port = getAvailablePort();

      Properties properties = new Properties();
      properties.setProperty("zookeeper.connect", host + ":" + zkClientPort);
      properties.setProperty("broker.id", String.valueOf(id));
      properties.setProperty("host.name", host);
      properties.setProperty("port", String.valueOf(port));
      properties.setProperty("delete.topic.enable", Boolean.toString(true));

      File dir = new File(workDir, "kafka-" + id).getAbsoluteFile();
      recreateDir(dir, clean);

      properties.setProperty("log.dir", dir.getAbsolutePath());
      properties.setProperty("log.flush.interval.messages", String.valueOf(1));

      brokers.add(startBroker(properties));
    }


    started = true;

    logger.info("Kafka Minicluster service started with " + numKafkaBrokers
        + " brokers.");
  }

  // from com.linkedin.camus.etl.kafka.KafkaCluster
  @Override
  public void stop() throws IOException {
    if (!started) {
      return;
    }

    for (KafkaServer broker : brokers) {
      broker.shutdown();
    }

    started = false;

    logger.info("Kafka Minicluster service shut down.");
  }

  @Override
  public List<Class<? extends Service>> dependencies() {
    List<Class<? extends Service>> services = new ArrayList<Class<? extends Service>>();
    services.add(ZookeeperService.class);
    services.add(SchemaRepoService.class);
    return services;
  }

  // from com.linkedin.camus.etl.kafka.KafkaCluster
  private static KafkaServer startBroker(Properties props) {
    KafkaServer server = new KafkaServer(new KafkaConfig(props), new SystemTime());
    server.startup();
    return server;
  }

  // from com.linkedin.camus.etl.kafka.KafkaCluster
  public static class SystemTime implements Time {

    @Override
    public long milliseconds() {
      return System.currentTimeMillis();
    }

    @Override
    public long nanoseconds() {
      return System.nanoTime();
    }

    @Override
    public void sleep(long ms) {
      try {
        Thread.sleep(ms);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void recreateDir(File dir, boolean clean) throws IOException {
    if (dir.exists() && clean) {
      FileUtil.fullyDelete(dir);
    } else if (dir.exists() && !clean) {
      // the directory's exist, and we don't want to clean, so exit
      return;
    }
    try {
      dir.mkdirs();
    } catch (SecurityException e) {
      throw new IOException("creating dir: " + dir, e);
    }
  }

  private static int getAvailablePort() throws IOException {
    ServerSocket socket = new ServerSocket(0);
    try {
      return socket.getLocalPort();
    } finally {
      socket.close();
    }
  }
}
