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

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.reflect.CustomEncoding;
import org.kitesdk.data.Format;
import org.kitesdk.data.Formats;


public class FormatAsString extends CustomEncoding<Format>{

  private static final String NULL = "";

  public FormatAsString() {
    schema = Schema.create(Schema.Type.STRING);
    schema.addProp("CustomEncoding", "FormatAsStringEncoding");
  }

  @Override
  protected void write(Object datum, Encoder out) throws IOException {
    String asString = NULL;
    if (datum != null) {
      if (datum instanceof Format) {
        Format format = (Format)datum;
        asString = format.getName();
      }
    }
    out.writeString(asString);
  }

  @Override
  protected Format read(Object reuse, Decoder in) throws IOException {
    String asString = in.readString();
    if (NULL.equals(asString)) {
      return null;
    } else {
      return Formats.fromString(asString);
    }
  }
}
