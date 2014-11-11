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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.avro.Schema;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.IncompatibleSchemaException;
import org.kitesdk.data.spi.AbstractMetadataProvider;
import org.kitesdk.data.spi.ColumnMappingParser;
import org.kitesdk.data.spi.JsonUtil;
import org.kitesdk.data.spi.PartitionStrategyParser;
import org.schemarepo.Repository;
import org.schemarepo.SchemaValidationException;
import org.schemarepo.Subject;
import org.schemarepo.SubjectConfig;

public class KafkaMetadataProvider extends AbstractMetadataProvider {

  private static final String PROPERTIES_KEY = "kite.dataset.properties";
  private static final String NAMESPACE_KEY = "kite.dataset.namespace";
  private static final String NAME_KEY = "kite.dataset.name";
  private final Repository repo;
  private final ZkClient zkClient;

  public KafkaMetadataProvider(Repository repo, ZkClient zkClient) {
    this.repo = repo;
    this.zkClient = zkClient;
  }

  @Override
  public DatasetDescriptor load(String namespace, String name) {
    Preconditions.checkArgument(exists(namespace, name),
        "Dataset %s.%s doesn't exist", namespace, name);

    Subject subject = repo.lookup(KafkaUtil.topicName(namespace, name));
    String schema = subject.latest().getSchema();

    DatasetDescriptor.Builder builder = new DatasetDescriptor.Builder()
        .schemaLiteral(schema);

    ObjectNode schemaJson = JsonUtil.parse(schema, ObjectNode.class); 
    JsonNode props = schemaJson.get(PROPERTIES_KEY);
    Iterator<Map.Entry<String, JsonNode>> fields = props.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      builder.property(field.getKey(), field.getValue().asText());
    }

    return builder.build();
  }

  @Override
  public DatasetDescriptor create(String namespace, String name, final DatasetDescriptor descriptor) {
    Preconditions.checkArgument(!exists(namespace, name),
        "Dataset %s.%s already exists", namespace, name);

    return updateOrCreate(namespace, name, descriptor);
  }

  @Override
  public DatasetDescriptor update(String namespace, String name, DatasetDescriptor descriptor) {
    Preconditions.checkArgument(exists(namespace, name),
        "Dataset %s.%s doesn't exist", namespace, name);

    return updateOrCreate(namespace, name, descriptor);
  }

  private DatasetDescriptor updateOrCreate(String namespace, String name, DatasetDescriptor descriptor) {
    String topic = KafkaUtil.topicName(namespace, name);

    Subject subject = repo.lookup(topic);
    if (subject == null) {
      SubjectConfig config = new SubjectConfig.Builder()
          .set(NAMESPACE_KEY, namespace)
          .set(NAME_KEY, namespace)
          .build();
      subject = repo.register(topic, config);
    }

    Schema schema = getEmbeddedSchema(descriptor);
    DatasetDescriptor.Builder builder = new DatasetDescriptor.Builder(descriptor)
        .schema(schema);

    try {
      String schemaId = subject.register(schema.toString()).getId();
      builder.property(KafkaConfig.KITE_SCHEMA_ID, schemaId);
    } catch (SchemaValidationException ex) {
      throw new IncompatibleSchemaException("Schema validation failed", ex);
    }

    return builder.build();
    
  }

  void createOrUpdateSchemaId(String namespace, String name, DatasetDescriptor descriptor) {
    String id = descriptor.getProperty(KafkaConfig.KITE_SCHEMA_ID);
    String schemaIdPath = ZkUtils.getTopicPath(KafkaUtil.topicName(namespace, name))
        + "/" + KafkaConfig.KITE_SCHEMA_ID;
    if (ZkUtils.pathExists(zkClient, schemaIdPath)) {
      ZkUtils.updatePersistentPath(zkClient, schemaIdPath, id);
    } else {
      ZkUtils.createPersistentPath(zkClient, schemaIdPath, id);
    }
  }

  @Override
  public boolean delete(String namespace, String name) {
    if (!exists(namespace, name)) {
      return false;
    }

    String schemaIdPath = ZkUtils.getTopicPath(KafkaUtil.topicName(namespace, name))
        + "/" + KafkaConfig.KITE_SCHEMA_ID;
    ZkUtils.deletePath(zkClient, schemaIdPath);

    return true;
  }

  @Override
  public boolean exists(String namespace, String name) {
    String schemaIdPath = ZkUtils.getTopicPath(KafkaUtil.topicName(namespace, name))
        + "/" + KafkaConfig.KITE_SCHEMA_ID;
    return ZkUtils.pathExists(zkClient, schemaIdPath);
  }

  @Override
  public Collection<String> namespaces() {
    Set<String> namespaces = new HashSet<String>();
    Iterable<Subject> subjects = repo.subjects();
    for (Subject subject : subjects) {
      String namespace = subject.getConfig().get(NAMESPACE_KEY);
      namespaces.add(namespace);
    }

    return namespaces;
  }

  @Override
  public Collection<String> datasets(String namespace) {
    Set<String> names = new HashSet<String>();
    Iterable<Subject> subjects = repo.subjects();
    for (Subject subject : subjects) {
      if (namespace.equals(subject.getConfig().get(NAMESPACE_KEY))) {
        names.add(subject.getConfig().get(NAME_KEY));
      }
    }

    return names;
  }

  private static Schema getEmbeddedSchema(DatasetDescriptor descriptor) {
    Schema schema = descriptor.getSchema();

    if (descriptor.isColumnMapped()) {
      schema = ColumnMappingParser
          .embedColumnMapping(schema, descriptor.getColumnMapping());
    }

    if (descriptor.isPartitioned()) {
      schema = PartitionStrategyParser
          .embedPartitionStrategy(schema, descriptor.getPartitionStrategy());
    }

    ObjectNode schemaJson = JsonUtil.parse(schema.toString(), ObjectNode.class);
    schemaJson.set(PROPERTIES_KEY, toJson(descriptor));
    schema = new Schema.Parser().parse(schemaJson.toString());

    return schema;
  }

  private static JsonNode toJson(DatasetDescriptor descriptor) {
    ObjectNode propertiesJson = JsonNodeFactory.instance.objectNode();
    for (String name : descriptor.listProperties()) {
      String value = descriptor.getProperty(name);
      propertiesJson.set(name, TextNode.valueOf(value));
    }

    return propertiesJson;
  }

}
