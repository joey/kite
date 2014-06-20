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

package org.kitesdk.data.remote.protocol.handle;

public abstract class Handle {

  private long id;

  public Handle() {
  }

  public Handle(long id) {
    this.id = id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public long getId() {
    return id;
  }

  @Override
  public String toString() {
    return String.format("%s{ id=%d}", getClass().getSimpleName(), id);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final Handle other = (Handle) obj;
    if (this.id != other.id) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 97 * hash + (int) (this.id ^ (this.id >>> 32));
    return hash;
  }
}
