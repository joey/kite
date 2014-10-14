/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data;

import java.util.ServiceLoader;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents how to store a Schema field.
 *
 * @since 0.14.0
 */
@Immutable
public abstract class FieldMapping {

  private static final Logger LOG = LoggerFactory.getLogger(FieldMapping.class);

  /**
   * The supported Mapping Types, which control how an entity field maps to
   * columns in an HBase table.
   *
   * @since 0.14.0
   */
  public static enum MappingType {

    // Maps a value to a part of the row key
    KEY,

    // Maps a value to a single column.
    COLUMN,

    // Maps a map or record value to columns
    // in a column family.
    KEY_AS_COLUMN,

    // Maps a field to one that can be incremented
    COUNTER,

    // The field will be populated with the
    // current version of the entity. This
    // allows the version to be checked if this
    // same entity is persisted back, to make sure
    // it hasn't changed.
    OCC_VERSION
  }

  private static String SYS_COL_FAMILY = "_s";
  private static String OCC_QUALIFIER = "w";

  public static FieldMapping key(String name) {
    return FIELD_MAPPING_FACTORY.newFieldMapping(name, MappingType.KEY, null, null, null);
  }

  public static FieldMapping column(String name, String family, String qualifier) {
    return FIELD_MAPPING_FACTORY.newFieldMapping(name, MappingType.COLUMN, family, qualifier, null);
  }

  public static FieldMapping keyAsColumn(String name, String family) {
    return FIELD_MAPPING_FACTORY.newFieldMapping(
        name, MappingType.KEY_AS_COLUMN, family, null, null);
  }

  public static FieldMapping keyAsColumn(String name, String family,
                                         @Nullable String qualifierPrefix) {
    return FIELD_MAPPING_FACTORY.newFieldMapping(
        name, MappingType.KEY_AS_COLUMN, family, null, qualifierPrefix);
  }

  public static FieldMapping counter(String name, String family, String qualifier) {
    return FIELD_MAPPING_FACTORY.newFieldMapping(name, MappingType.COUNTER, family, qualifier, null);
  }

  public static FieldMapping occ(String name) {
    return FIELD_MAPPING_FACTORY.newFieldMapping(
        name, MappingType.OCC_VERSION, SYS_COL_FAMILY, OCC_QUALIFIER, null);
  }

  public static FieldMapping version(String name) {
    return occ(name);
  }

  public abstract String getFieldName();

  public abstract MappingType getMappingType();

  public abstract String getPrefix();

  public abstract byte[] getFamily();

  public abstract String getFamilyAsString();

  public abstract byte[] getQualifier();

  public abstract String getQualifierAsString();

  private final static FieldMappingFactory FIELD_MAPPING_FACTORY;

  protected static interface FieldMappingFactory {

    public FieldMapping newFieldMapping(String fieldName,
        MappingType mappingType, @Nullable String family,
        @Nullable String qualifier, @Nullable String prefix);

  }

  static {
    ServiceLoader<FieldMappingFactory> factories =
        ServiceLoader.load(FieldMappingFactory.class);

    FieldMappingFactory selectedFactory = null;
    for (FieldMappingFactory factory : factories) {
      LOG.debug("Using {} to build FieldMapping objects", factory.getClass());
      selectedFactory = factory;
      break;
    }

    if (selectedFactory == null) {
      throw new RuntimeException("No implementation of " + FieldMapping.class +
          " available. Make sure that kite-data-common is on the classpath");
    }

    FIELD_MAPPING_FACTORY = selectedFactory;
  }

}
