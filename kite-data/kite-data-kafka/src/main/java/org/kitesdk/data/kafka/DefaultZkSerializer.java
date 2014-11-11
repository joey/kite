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

import kafka.utils.ZKStringSerializer;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

public class DefaultZkSerializer implements ZkSerializer {

  @Override
  public byte[] serialize(Object data) throws ZkMarshallingError {
    return ZKStringSerializer.serialize(data);
  }

  @Override
  public Object deserialize(byte[] bytes) throws ZkMarshallingError {
    return ZKStringSerializer.deserialize(bytes);
  }

}
