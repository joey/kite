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
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.FieldMapping;
import org.kitesdk.data.spi.ColumnMappingParser;

/**
 * Represents how to store a Schema field.
 *
 * @since 0.18.0
 */
@Immutable
public class FieldMappingImpl extends FieldMapping {

  private final String fieldName;
  private final MappingType mappingType;
  private final String prefix;
  private final String familyString;
  private final byte[] family;
  private final String qualifierString;
  private final byte[] qualifier;

  private FieldMappingImpl(String fieldName, MappingType mappingType,
      @Nullable String family, @Nullable String qualifier,
      @Nullable String prefix) {
    this.fieldName = fieldName;
    this.mappingType = mappingType;
    this.familyString = family;
    if (family != null) {
      this.family = encodeUtf8(family);
    } else {
      this.family = null;
    }
    this.qualifierString = qualifier;
    if (qualifier != null) {
      this.qualifier = encodeUtf8(qualifier);
    } else {
      this.qualifier = null;
    }
    if (prefix != null && !prefix.isEmpty()) {
      this.prefix = prefix;
    } else {
      this.prefix = null;
    }
  }

  @Override
  public String getFieldName() {
    return fieldName;
  }

  @Override
  public MappingType getMappingType() {
    return mappingType;
  }

  @Override
  public String getPrefix() {
    return prefix;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="EI_EXPOSE_REP",
      justification="Defensive copy is needlessly expensive")
  @Override
  public byte[] getFamily() {
    return family;
  }

  @Override
  public String getFamilyAsString() {
    return familyString;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="EI_EXPOSE_REP",
      justification="Defensive copy is needlessly expensive")
  @Override
  public byte[] getQualifier() {
    return qualifier;
  }

  @Override
  public String getQualifierAsString() {
    return qualifierString;
  }

  private static byte[] encodeUtf8(String str) {
    if (str == null) {
      return new byte[0];
    }
    try {
      return str.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new DatasetIOException("[FATAL] Cannot decode UTF-8", e);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        fieldName, mappingType, Arrays.hashCode(family),
        Arrays.hashCode(qualifier), prefix);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    FieldMappingImpl other = (FieldMappingImpl) obj;
    return (Objects.equal(fieldName, other.fieldName) &&
        Objects.equal(mappingType, other.mappingType) &&
        Arrays.equals(family, other.family) &&
        Arrays.equals(qualifier, other.qualifier) &&
        Objects.equal(prefix, other.prefix));
  }

  @Override
  public String toString() {
    return ColumnMappingParser.toString(this);
  }

  static final class FieldMappingImplFactory implements FieldMappingFactory {

    @Override
    public FieldMapping newFieldMapping(String fieldName,
        MappingType mappingType, @Nullable String family,
        @Nullable String qualifier, @Nullable String prefix) {
      return new FieldMappingImpl(fieldName, mappingType, family, qualifier, prefix);
    }

  }
}
