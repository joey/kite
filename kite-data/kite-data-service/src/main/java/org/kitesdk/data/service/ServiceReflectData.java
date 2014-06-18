/*
 * Copyright 2014 joey.
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

package org.kitesdk.data.service;

import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;

class ServiceReflectData extends ReflectData {

  private Class<?> entityClass;

  public <E> ServiceReflectData(Class<?> iface, Class<?> entityClass) {
    super(iface.getClassLoader());
    this.entityClass = entityClass;
  }

  @Override
  protected Schema createSchema(Type type, Map<String, Schema> names) {
    if (type instanceof TypeVariable) {
      TypeVariable typeVariable = (TypeVariable) type;
      if ("E".equals(typeVariable.getName())) {
        System.err.println(typeVariable.getName());
        type = entityClass;
      }
    }
    return super.createSchema(type, names);
  }

}
