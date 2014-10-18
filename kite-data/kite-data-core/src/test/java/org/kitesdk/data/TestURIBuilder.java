/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data;

import java.net.URI;
import java.net.URLEncoder;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kitesdk.data.spi.Constraints;

public class TestURIBuilder {
  private static final Schema SCHEMA = SchemaBuilder.record("Event").fields()
      .requiredString("uid")
      .requiredLong("timestamp")
      .requiredString("color")
      .endRecord();

  private static final PartitionStrategy STRATEGY = new PartitionStrategy.Builder()
      .hash("uid", "uid-hash", 64)
      .year("timestamp")
      .month("timestamp")
      .day("timestamp")
      .identity("uid")
      .build();

  private static final String ID = UUID.randomUUID().toString();

  private static final Constraints empty = new Constraints(SCHEMA, STRATEGY);

  @BeforeClass
  public static void setupMockRepositories() {
    MockRepositories.newMockRepository();
  }

  @Test
  public void testRepoUriAndNameToDatasetUri() {
    Assert.assertEquals("Should construct the correct dataset URI",
        URI.create("dataset:mock:0/ns/test-name"),
        new URIBuilder("repo:mock:0", "ns", "test-name").build());
  }

  @Test
  public void testRepoUriAndNameConstructorRejectsBadUris() {
    TestHelpers.assertThrows("Should reject dataset: URI",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            new URIBuilder("dataset:mock:0/test-name", "ns", "test-name-2")
                .build();
          }
        });
    TestHelpers.assertThrows("Should reject view: URI",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            new URIBuilder("view:mock:0/test-name?n=34", "ns", "test-name-2")
                .build();
          }
        });
    TestHelpers.assertThrows("Should reject null URI",
        NullPointerException.class, new Runnable() {
          @Override
          public void run() {
            new URIBuilder((String) null, "ns", "test-name").build();
          }
        });
    TestHelpers.assertThrows("Should reject null URI",
        NullPointerException.class, new Runnable() {
          @Override
          public void run() {
            new URIBuilder((URI) null, "ns", "test-name").build();
          }
        });
    TestHelpers.assertThrows("Should reject null namespace",
        NullPointerException.class, new Runnable() {
          @Override
          public void run() {
            new URIBuilder("repo:mock:0", null, "test-name").build();
          }
        });
    TestHelpers.assertThrows("Should reject null name",
        NullPointerException.class, new Runnable() {
          @Override
          public void run() {
            new URIBuilder("repo:mock:0", "ns", null).build();
          }
        });
  }

  @Test
  public void testRepoUriAndNameToDatasetUriPreservesOptions() {
    Assert.assertEquals("Should construct the correct dataset URI",
        URI.create("dataset:mock:0/ns/test-name?hdfs:port=1080"),
        new URIBuilder("repo:mock:0?hdfs:port=1080", "ns", "test-name")
            .build());
  }

  @Test
  public void testRepoUriAndNameAddEquals() {
    Assert.assertEquals("Should construct the correct dataset URI",
        URI.create("view:mock:0/ns/test-name?prop=value"),
        new URIBuilder("repo:mock:0", "ns", "test-name")
            .with("prop", "value")
            .build());
    // order should be preserved
    Assert.assertEquals("Should construct the correct dataset URI",
        URI.create("view:mock:0/ns/test-name?prop=value&num=34"),
        new URIBuilder("repo:mock:0", "ns", "test-name")
            .with("prop", "value")
            .with("num", 34)
            .build());
  }

  @Test
  public void testDatasetUriToDatasetUri() {
    Assert.assertEquals("Should produce an equivalent dataset URI",
        URI.create("dataset:mock:0/ns/test-name"),
        new URIBuilder("dataset:mock:0/ns/test-name").build());
  }

  @Test
  public void testDatasetUriConstructorRejectsBadUris() {
    TestHelpers.assertThrows("Should reject repo: URI",
        IllegalArgumentException.class, new Runnable() {
          @Override
          public void run() {
            new URIBuilder("repo:mock:0/test-name").build();
          }
        });
    TestHelpers.assertThrows("Should reject null URI",
        NullPointerException.class, new Runnable() {
          @Override
          public void run() {
            new URIBuilder((String) null).build();
          }
        });
    TestHelpers.assertThrows("Should reject null URI",
        NullPointerException.class, new Runnable() {
          @Override
          public void run() {
            new URIBuilder((URI) null).build();
          }
        });
  }

  @Test
  public void testDatasetUriToDatasetUriPreservesOptions() {
    // this doesn't produce a view URI because the original isn't a view URI
    Assert.assertEquals("Should construct the correct dataset URI",
        URI.create("dataset:mock:0/ns/test-name?hdfs:port=1080"),
        new URIBuilder("dataset:mock:0/ns/test-name?hdfs:port=1080")
            .build());
  }

  @Test
  public void testDatasetUriAddEquals() {
    Assert.assertEquals("Should produce an equivalent dataset URI",
        URI.create("view:mock:0/ns/test-name?prop=value"),
        new URIBuilder("dataset:mock:0/ns/test-name")
            .with("prop", "value")
            .build());
    Assert.assertEquals("Should produce an equivalent dataset URI",
        URI.create("view:mock:0/ns/test-name?prop=value&num=34"),
        new URIBuilder("dataset:mock:0/ns/test-name")
            .with("prop", "value")
            .with("num", 34)
            .build());
  }

  @Test
  public void testViewUriToViewUri() {
    Assert.assertEquals("Should produce an equivalent view URI",
        URI.create("view:mock:0/ns/test-name?prop=value"),
        new URIBuilder("view:mock:0/ns/test-name?prop=value").build());
  }

  @Test
  public void testViewUriAddEquals() throws Exception {
    Assert.assertEquals("Should produce an equivalent dataset URI",
        URI.create("view:mock:0/ns/test-name?prop=value&field=v2"),
        new URIBuilder("view:mock:0/ns/test-name?prop=value")
            .with("field", "v2")
            .build());
    Assert.assertEquals("Should produce an equivalent dataset URI",
        URI.create("view:mock:0/ns/test-name?prop=value&field=v2&num=34"),
        new URIBuilder("view:mock:0/ns/test-name?prop=value")
            .with("field", "v2")
            .with("num", 34)
            .build());
    Assert.assertEquals("Should produce an equivalent dataset URI",
        URI.create("view:mock:0/ns/test-name?field=a/b"),
        new URIBuilder("view:mock:0/ns/test-name")
            .with("field", "a/b")
            .build());
    Assert.assertEquals("Should produce an equivalent dataset URI",
        URI.create("view:mock:0/ns/test-name?field=a%2Fb"),
        new URIBuilder("view:mock:0/ns/test-name")
            .with("field", URLEncoder.encode("a/b", "UTF-8"))
            .build());
  }

  @Test
  public void testAddEqualityConstraints() {
    Assert.assertEquals("Should add equality constraints",
        URI.create("view:mock:0/ns/test?uid=" + ID),
        new URIBuilder("dataset:mock:0/ns/test")
            .constraints(empty.with("uid", new Utf8(ID)))
                .build());
    Assert.assertEquals("Should add equality constraints",
        URI.create("view:mock:0/ns/test?uid=a,b"),
        new URIBuilder("dataset:mock:0/ns/test")
            .constraints(empty.with("uid", new Utf8("a"), new Utf8("b")))
            .build());
    Assert.assertEquals("Should add equality constraints",
        URI.create("view:mock:0/ns/test?uid=" + ID + "&timestamp=1405720705333"),
        new URIBuilder("dataset:mock:0/ns/test")
            .constraints(
                empty.with("uid", new Utf8(ID)).with("timestamp", 1405720705333L))
            .build());
    Assert.assertEquals("Should add encoded equality constraints",
        URI.create("view:mock:0/ns/test?uid=a%2Fb"),
        new URIBuilder("dataset:mock:0/ns/test")
            .constraints(empty.with("uid", new Utf8("a/b")))
            .build());
  }

  @Test
  public void testAddExistsConstraints() {
    Assert.assertEquals("Should add equality constraints",
        URI.create("view:mock:0/ns/test?uid="),
        new URIBuilder("dataset:mock:0/ns/test")
            .constraints(empty.with("uid"))
            .build());
    Assert.assertEquals("Should add equality constraints",
        URI.create("view:mock:0/ns/test?uid=&timestamp="),
        new URIBuilder("dataset:mock:0/ns/test")
            .constraints(empty.with("uid").with("timestamp"))
            .build());
  }

  @Test
  public void testAddRangeConstraints() {
    Assert.assertEquals("Should add equality constraints",
        URI.create("view:mock:0/ns/test?color=[green,)"),
        new URIBuilder("dataset:mock:0/ns/test")
            .constraints(empty.from("color", "green"))
            .build());
    Assert.assertEquals("Should add equality constraints",
        URI.create("view:mock:0/ns/test?color=(,green]"),
        new URIBuilder("dataset:mock:0/ns/test")
            .constraints(empty.to("color", "green"))
            .build());
    Assert.assertEquals("Should add equality constraints",
        URI.create("view:mock:0/ns/test?timestamp=[0,1405720705333)&color=(green,red]"),
        new URIBuilder("dataset:mock:0/ns/test")
            .constraints(empty
                .from("timestamp", 0l).toBefore("timestamp", 1405720705333L)
                .fromAfter("color", "green").to("color", "red"))
            .build());
  }

  @Test
  public void testEmptyConstraints() {
    Assert.assertEquals("Empty constraints should produce dataset URI",
        URI.create("dataset:mock:0/ns/test"),
        new URIBuilder("dataset:mock:0/ns/test")
            .constraints(empty)
            .build());
  }
}
