/**
 * Copyright 2014 Cloudera Inc.
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
package org.kitesdk.data.impl;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.kitesdk.data.ColumnMapping;
import org.kitesdk.data.FieldMapping;
import org.kitesdk.data.spi.ColumnMappingParser;

/**
 * A descriptor for an entity's column mappings, which defines how an entity
 * maps to a columnar store.
 *
 * @since 0.18.0
 */
@Immutable
public class ColumnMappingImpl extends ColumnMapping {

  private final Collection<FieldMapping> fieldMappings;

  private ColumnMappingImpl(Collection<FieldMapping> mappings) {
    fieldMappings = ImmutableList.copyOf(mappings);
  }

  @Override
  public Collection<FieldMapping> getFieldMappings() {
    return fieldMappings;
  }

  @Override
  public FieldMapping getFieldMapping(String fieldName) {
    for (FieldMapping fm : fieldMappings) {
      if (fm.getFieldName().equals(fieldName)) {
        return fm;
      }
    }
    return null;
  }

  /**
   * Get the columns required by this schema.
   *
   * @return The set of columns
   */
  @Override
  public Set<String> getRequiredColumns() {
    Set<String> set = new HashSet<String>();
    for (FieldMapping fieldMapping : fieldMappings) {
      if (FieldMapping.MappingType.KEY == fieldMapping.getMappingType()) {
        continue;
      } else if (FieldMapping.MappingType.KEY_AS_COLUMN == fieldMapping.getMappingType()) {
        set.add(fieldMapping.getFamilyAsString() + ":");
      } else {
        set.add(fieldMapping.getFamilyAsString() + ":"
            + fieldMapping.getQualifierAsString());
      }
    }
    return set;
  }

  /**
   * Get the column families required by this schema.
   *
   * @return The set of column families.
   */
  @Override
  public Set<String> getRequiredColumnFamilies() {
    Set<String> set = new HashSet<String>();
    for (FieldMapping mapping : fieldMappings) {
      if (FieldMapping.MappingType.KEY != mapping.getMappingType())
      set.add(mapping.getFamilyAsString());
    }
    return set;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ColumnMappingImpl that = (ColumnMappingImpl) o;
    return Objects.equal(fieldMappings, that.fieldMappings);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(fieldMappings.hashCode());
  }

  @Override
  public String toString() {
    return ColumnMappingParser.toString(this, false);
  }

  @Override
  public String toString(boolean pretty) {
    return ColumnMappingParser.toString(this, pretty);
  }

  static class ColumnMappingImplFactory implements ColumnMappingFactory {

    @Override
    public ColumnMapping newColumnMapping(Collection<FieldMapping> fieldMappings) {
      return new ColumnMappingImpl(fieldMappings);
    }

  }

}
