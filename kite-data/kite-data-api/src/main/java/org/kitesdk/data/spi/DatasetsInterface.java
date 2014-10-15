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
package org.kitesdk.data.spi;

import java.net.URI;
import java.util.Collection;
import org.apache.avro.generic.GenericRecord;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.View;


/**
 * Definition of the {@link Datasets} interface for service providers.
 * 
 * User code should use the static methods in {@link Datasets} rather than
 * using this interface directly.
 * 
 * @since 0.18.0
 */
public interface DatasetsInterface {

  /**
   * Create a {@link Dataset} for the given dataset or view URI.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uri a {@code Dataset} or {@code View} URI.
   * @param type the Java type of the entities in the dataset
   * @param <E> The type of entities stored in the {@code Dataset}.
   * @param <V> The type of {@code Dataset} or {@code View} expected.
   * @return a newly created {@code Dataset} responsible for the given URI.
   */
  public <E, V extends View<E>> V create(URI uri, DatasetDescriptor descriptor, Class<E> type);

  /**
   * Create a {@link Dataset} for the given dataset or view URI.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uri a {@code Dataset} or {@code View} URI.
   * @param <V> The type of {@code Dataset} or {@code View} expected.
   * @return a newly created {@code Dataset} responsible for the given URI.
   */
  public <V extends View<GenericRecord>> V create(URI uri, DatasetDescriptor descriptor);

  /**
   * Create a {@link Dataset} for the given dataset or view URI string.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uri a {@code Dataset} or {@code View} URI string.
   * @param type the Java type of the entities in the dataset
   * @param <E> The type of entities stored in the {@code Dataset}.
   * @param <V> The type of {@code Dataset} or {@code View} expected.
   * @return a newly created {@code Dataset} responsible for the given URI.
   */
  public <E, V extends View<E>> V create(String uri, DatasetDescriptor descriptor, Class<E> type);

  /**
   * Create a {@link Dataset} for the given dataset or view URI string.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uri a {@code Dataset} or {@code View} URI string.
   * @param <V> The type of {@code Dataset} or {@code View} expected.
   * @return a newly created {@code Dataset} responsible for the given URI.
   */
  public <V extends View<GenericRecord>> V create(String uri, DatasetDescriptor descriptor);

  /**
   * Delete a {@link Dataset} identified by the given dataset URI.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:".
   *
   * @param uri a {@code Dataset} URI.
   * @return {@code true} if any data or metadata was removed, or {@code false}
   */
  boolean delete(URI uri);

  /**
   * Delete a {@link Dataset} identified by the given dataset URI string.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:".
   *
   * @param uri a {@code Dataset} URI string.
   * @return {@code true} if any data or metadata was removed, or {@code false}
   */
  boolean delete(String uri);

  /**
   * Check if a {@link Dataset} identified by the given URI exists.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:".
   *
   * @param uri a {@code Dataset} URI.
   * @return {@code true} if the dataset exists, {@code false} otherwise
   */
  boolean exists(URI uri);

  /**
   * Check if a {@link Dataset} identified by the given URI string exists.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:".
   *
   * @param uri a {@code Dataset} URI string.
   * @return {@code true} if the dataset exists, {@code false} otherwise
   */
  boolean exists(String uri);

  /**
   * List the {@link Dataset} URIs in the repository identified by the URI
   * <p>
   * URI formats are defined by {@code Dataset} implementations. The repository
   * URIs passed to this method must begin with "repo:".
   *
   * @param uri a {@code DatasetRepository} URI
   * @return the URIs present in the {@code DatasetRepository}
   */
  Collection<URI> list(URI uri);

  /**
   * List the {@link Dataset} URIs in the repository identified by the URI string
   * <p>
   * URI formats are defined by {@code Dataset} implementations. The repository
   * URIs passed to this method must begin with "repo:".
   *
   * @param uri a {@code DatasetRepository} URI string
   * @return the URIs present in the {@code DatasetRepository}
   */
  Collection<URI> list(String uri);

  /**
   * Load a {@link Dataset} or {@link View} for the given {@link URI}.
   * <p>
   * If the URI is a dataset URI, the unfiltered Dataset will be returned.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uri a {@code Dataset} or {@code View} URI.
   * @param type the Java type of the entities in the dataset
   * @param <E> The type of entities stored in the {@code Dataset}.
   * @param <V> The type of {@code View} expected.
   * @return a {@code View} for the given URI.
   */
  public <E, V extends View<E>> V load(URI uri, Class<E> type);

  /**
   * Load a {@link Dataset} or {@link View} for the given {@link URI}.
   * <p>
   * If the URI is a dataset URI, the unfiltered Dataset will be returned.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uri a {@code Dataset} or {@code View} URI.
   * @param <V> The type of {@code View} expected.
   * @return a {@code View} for the given URI.
   */
  public <V extends View<GenericRecord>> V load(URI uri);

  /**
   * Load a {@link Dataset} or {@link View} for the given URI string.
   * <p>
   * If the URI is a dataset URI, the unfiltered Dataset will be returned.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uriString a {@code Dataset} or {@code View} URI.
   * @param type the Java type of the entities in the dataset
   * @param <E> The type of entities stored in the {@code Dataset}.
   * @param <V> The type of {@code View} expected.
   * @return a {@code View} for the given URI.
   */
  public <E, V extends View<E>> V load(String uriString, Class<E> type);

  /**
   * Load a {@link Dataset} or {@link View} for the given URI string.
   * <p>
   * If the URI is a dataset URI, the unfiltered Dataset will be returned.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uriString a {@code Dataset} or {@code View} URI.
   * @param <V> The type of {@code View} expected.
   * @return a {@code View} for the given URI.
   */
  public <V extends View<GenericRecord>> V load(String uriString);

  /**
   * Update a {@link Dataset} for the given dataset or view URI.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uri a {@code Dataset} or {@code View} URI.
   * @param type the Java type of the entities in the dataset
   * @param <E> The type of entities stored in the {@code Dataset}.
   * @param <D> The type of {@code Dataset} expected.
   * @return a newly created {@code Dataset} responsible for the given URI.
   */
  public <E, D extends Dataset<E>> D update(URI uri, DatasetDescriptor descriptor, Class<E> type);

  /**
   * Update a {@link Dataset} for the given dataset or view URI.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uri a {@code Dataset} or {@code View} URI.
   * @param <D> The type of {@code Dataset} expected.
   * @return a newly created {@code Dataset} responsible for the given URI.
   */
  public <D extends Dataset<GenericRecord>> D update(URI uri, DatasetDescriptor descriptor);

  /**
   * Update a {@link Dataset} for the given dataset or view URI string.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uri a {@code Dataset} or {@code View} URI string.
   * @param type the Java type of the entities in the dataset
   * @param <E> The type of entities stored in the {@code Dataset}.
   * @param <D> The type of {@code Dataset} expected.
   * @return a newly created {@code Dataset} responsible for the given URI.
   */
  public <E, D extends Dataset<E>> D update(String uri, DatasetDescriptor descriptor, Class<E> type);

  /**
   * Update a {@link Dataset} for the given dataset or view URI string.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uri a {@code Dataset} or {@code View} URI string.
   * @param <D> The type of {@code Dataset} expected.
   * @return a newly created {@code Dataset} responsible for the given URI.
   */
  public <D extends Dataset<GenericRecord>> D update(String uri, DatasetDescriptor descriptor);
  
}
