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

package org.kitesdk.data.remote.service;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Protocol;
import org.apache.avro.Protocol.Message;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.ipc.reflect.ReflectResponder;
import org.apache.avro.reflect.ReflectData;

public class ServiceReflectResponder extends ReflectResponder {

  private Object impl;
  private Schema schema;

  public ServiceReflectResponder(Class iface, Object impl, Schema schema) {
    super(iface, impl);
    this.impl = impl;
    this.schema = schema;
  }

  public ServiceReflectResponder(Protocol protocol, Object impl, Schema schema) {
    super(protocol, impl);
    this.impl = impl;
    this.schema = schema;
  }

  public ServiceReflectResponder(Class iface, Object impl, ReflectData data, Schema schema) {
    super(iface, impl, data);
    this.impl = impl;
    this.schema = schema;
  }

  public ServiceReflectResponder(Protocol protocol, Object impl, ReflectData data, Schema schema) {
    super(protocol, impl, data);
    this.impl = impl;
    this.schema = schema;
  }

  @Override
  @SuppressFBWarnings("DP_DO_INSIDE_DO_PRIVILEGED")
  public Object respond(Message message, Object request) throws Exception {
        int numParams = message.getRequest().getFields().size();
    Object[] params = new Object[numParams];
    Class[] paramTypes = new Class[numParams];
    int i = 0;
    try {
      for (Schema.Field param: message.getRequest().getFields()) {
        params[i] = ((GenericRecord)request).get(param.name());
        Schema paramSchema = param.schema();
        if (paramSchema.equals(schema)) {
          paramTypes[i] = Object.class;
        } else {
          paramTypes[i] = getSpecificData().getClass(param.schema());
        }
        i++;
      }
      Method method = impl.getClass().getMethod(message.getName(), paramTypes);
      method.setAccessible(true);
      return method.invoke(impl, params);
    } catch (InvocationTargetException e) {
      Throwable target = e.getTargetException();
      if (target instanceof Exception) {
        throw (Exception) target;
      } else {
        throw new Exception(target);
      }
    } catch (NoSuchMethodException e) {
      throw new AvroRuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new AvroRuntimeException(e);
    }
  }
}
