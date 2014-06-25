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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.apache.avro.AvroRuntimeException;
import org.slf4j.LoggerFactory;

abstract class RemoteAvroClient {

  private static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(RemoteAvroClient.class);

  @SuppressWarnings("unchecked")
  protected void handleAvroRuntimeException(AvroRuntimeException ex)
      throws RuntimeException {
    Throwable cause = ex.getCause();
    if (cause instanceof RuntimeException) {
      throw (RuntimeException)cause;
    } else {
      String message = ex.getMessage();
      String[] parts = message.split(":", 2);
      String className = parts[0];
      if (parts.length == 2) {
        message = parts[1].trim();
      } else {
        message = null;
      }
      Class<?> clazz;
      try {
        clazz = Class.forName(className);
        if (RuntimeException.class.isAssignableFrom(clazz)) {
          try {
            if (message == null) {
              throw ((Class<RuntimeException>)clazz).newInstance();
            } else {
              Constructor<RuntimeException> declaredConstructor =
                  ((Class<RuntimeException>)clazz).getDeclaredConstructor(String.class);
              throw declaredConstructor.newInstance(message);
            }
          } catch (NoSuchMethodException ex1) {
            LOG.debug("No {}(String message) constructor for class {}. Details may be lost: {}",
                new Object[] {clazz.getSimpleName(), className, message});
            try {
              throw ((Class<RuntimeException>)clazz).newInstance();
            } catch (InstantiationException ex2) {
              LOG.debug("Exception while instantiating {}", className);
              LOG.debug("", ex2);
            } catch (IllegalAccessException ex2) {
              LOG.debug("Exception while instantiating {}", className);
              LOG.debug("", ex2);
            }
          } catch (InstantiationException ex1) {
            LOG.debug("Exception while instantiating {}", className);
            LOG.debug("", ex1);
          } catch (IllegalAccessException ex1) {
            LOG.debug("Exception while instantiating {}", className);
            LOG.debug("", ex1);
          } catch (IllegalArgumentException ex1) {
            LOG.debug("Exception while instantiating {}", className);
            LOG.debug("", ex1);
          } catch (InvocationTargetException ex1) {
            LOG.debug("Exception while instantiating {}", className);
            LOG.debug("", ex1);
          }
        }
      } catch (ClassNotFoundException ex1) {
        LOG.debug("Exception while looking for class {}", className);
        LOG.debug("", ex1);
      }
    }
  }

}
