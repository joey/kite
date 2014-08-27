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

package org.kitesdk.data.spi;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.reflect.CustomEncoding;

public class SchemaAsStringEncoding extends CustomEncoding<Schema> {

  private static final String NULL = "";

  public SchemaAsStringEncoding() {
    schema = Schema.create(Schema.Type.STRING);
    schema.addProp("CustomEncoding", "SchemaAsStringEncoding");
  }

  @Override
  protected void write(Object datum, Encoder out) throws IOException {
    String asString = NULL;
    if (datum != null) {
      asString = datum.toString();
    }
    out.writeString(asString);
  }

  @Override
  protected Schema read(Object reuse, Decoder in) throws IOException {
    Parser parser = new Parser();
    String asString = in.readString();
    if (NULL.equals(asString)) {
      return null;
    } else {
      return parser.parse(asString);
    }
  }
}