/**
 * Copyright 2013 Cloudera Inc.
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
package org.kitesdk.data;

import java.util.ServiceLoader;
import org.kitesdk.data.spi.KeyBuilderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * A key for retrieving entities from a {@link RandomAccessDataset}.
 * </p>
 *
 * @since 0.9.0
 */
public abstract class Key {

  private static final Logger LOG = LoggerFactory.getLogger(Key.class);

  /**
   * Returns the value for {@code index}.
   *
   * @param index the {@code index} of the value to return
   * @return the Object stored at {@code index}
   */
  public abstract Object get(int index);

  /**
   * A fluent builder to aid in the construction of {@link Key} objects.
   *
   * @since 0.9.0
   */
  public static class Builder {

    private final Builder delegate;

    /**
     * Construct a {@link Builder} for a {@link RandomAccessDataset}.
     */
    public Builder(RandomAccessDataset dataset) {
      delegate = KEY_BUILDER_FACTORY.newKeyBuilder(dataset);
    }

    /**
     * No argument constructor for sub-classes
     */
    protected Builder() {
      delegate = null;
    }

    /**
     * Add a key value for the named field.
     *
     * @throws IllegalArgumentException If the there is no key field named
     * <code>name</code> for this builder's dataset.
     * @return An instance of the builder for method chaining.
     */
    public Builder add(String name, Object value) {
      return delegate.add(name, value);
    }

    /**
     * Build an instance of the configured key.
     *
     * @throws IllegalStateException If any required key field is missing.
     */
    public Key build() {
      return delegate.build();
    }

    private static final KeyBuilderFactory KEY_BUILDER_FACTORY;


    static {
      ServiceLoader<KeyBuilderFactory> factories
          = ServiceLoader.load(KeyBuilderFactory.class);

      KeyBuilderFactory selectedFactory = null;
      for (KeyBuilderFactory factory : factories) {
        LOG.debug("Using {} to build Key objects", factory.getClass());
        selectedFactory = factory;
        break;
      }

      if (selectedFactory == null) {
        String msg = "No implementation of " + Key.class + " available. Make"
           + " sure that kite-data-common is on the classpath";
        LOG.error(msg);
        throw new RuntimeException(msg);
      }

      KEY_BUILDER_FACTORY = selectedFactory;
    }

  }

}
