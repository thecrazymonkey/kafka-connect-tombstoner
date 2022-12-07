/*
 * Copyright © 2022 Ivan Kunz
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.thecrazymonkey.kafka.connect.smt.tombstoner;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import static org.junit.Assert.*;

public class TombstonerTest {

  private Tombstoner.Value<SinkRecord> xform = new Tombstoner.Value<>();
  private Tombstoner.Key<SinkRecord> xformKey = new Tombstoner.Key<>();

  private final SinkRecord recordWithoutSchema = new SinkRecord(
      "test",
      0,
      null,
      new HashMap<String, Object>(){{
        put("field", "field");
        put( "f1",
          new HashMap<String, Object>(){{
            put("field", "false");
            put("f4", "DELETE");
          }}
        );
      }},
      null,
      new HashMap<String, Object>(){{
        put("field", "DELETE");
        put( "f1",
          new HashMap<String, Object>(){{
            put("field", "false");
            put("f3", "DELETE");
          }}
        );
      }},
      0
  );

  private final Schema nestedStructKeySchema = SchemaBuilder.struct()
      .field("field", Schema.STRING_SCHEMA)
      .field("f4", Schema.STRING_SCHEMA)
      .build();

  private final Schema nestedStructValueSchema = SchemaBuilder.struct()
      .field("field", Schema.STRING_SCHEMA)
      .field("f3", Schema.STRING_SCHEMA)
      .build();

  private final Schema nestedKeyMapSchema =
      SchemaBuilder.map(Schema.STRING_SCHEMA, nestedStructKeySchema)
      .build();

  private final Schema nestedValueMapSchema =
      SchemaBuilder.map(Schema.STRING_SCHEMA, nestedStructValueSchema)
      .build();

  private final Schema keyschema =
      SchemaBuilder.struct()
        .field("field", Schema.STRING_SCHEMA)
        .field("f1", nestedKeyMapSchema)
        .build();

  private final Schema valueschema =
        SchemaBuilder.struct()
          .field("field", Schema.STRING_SCHEMA)
          .field("f1", nestedValueMapSchema)
          .build();
  
  
  private final SinkRecord recordWithSchema = new SinkRecord(
      "test",
      0,
      keyschema,
      new Struct(keyschema){{
        put("field", "field");
        put( "f1",
          new HashMap<String, Object>() {{
            put("field", new Struct(nestedStructKeySchema) {{
              put("field", "a");
              put("f4", "DELETE");
            }});
          }}
        );
      }},
      valueschema,
      new Struct(valueschema){{
        put("field", "DELETE");
        put( "f1",
          new HashMap<String, Object>() {{
            put("field", new Struct(nestedStructValueSchema) {{
              put("field", "a");
              put("f3", "DELETE");
            }});
          }}
        );
      }},
      0
  );

  @After
  public void teardown() {
    xform.close();
  }

  @Test
  public void insertTombstoneOnKeyWithoutSchema() {
    xformKey.configure(new HashMap<String, Object>(){{
      put(Tombstoner.TOMBSTONER_FIELD, "$.f1.[?(@.f4 == 'DELETE')]");
    }});

    final SinkRecord transformedRecord = xformKey.apply(recordWithoutSchema);
    assertNull("Tombstone record not inserted.", transformedRecord.value());
  }

  @Test
  public void noInsertTombstoneOnKeyWithoutSchema() {
    xformKey.configure(new HashMap<String, Object>(){{
      put(Tombstoner.TOMBSTONER_FIELD, "$.f1.[?(@.field == 'DELETE')]");
    }});

    final SinkRecord transformedRecord = xformKey.apply(recordWithoutSchema);
    assertEquals("Tombstone record inserted incorrectly.", recordWithoutSchema.value(), transformedRecord.value());
  }

  @Test
  public void noInsertTombstoneOnInvalidPathKey() {
    xformKey.configure(new HashMap<String, Object>(){{
      put(Tombstoner.TOMBSTONER_FIELD, "$.field.[?(@.field == 'DELETE')]");
    }});

    final SinkRecord transformedRecord = xformKey.apply(recordWithoutSchema);
    assertEquals("Tombstone record inserted incorrectly on invalid key path.", recordWithoutSchema.value(), transformedRecord.value());
  }

  @Test
  public void insertTombstoneOnValueWithoutSchema() {
    xform.configure(new HashMap<String, Object>(){{
      put(Tombstoner.TOMBSTONER_FIELD, "$.f1[?(@.f3 == 'DELETE')]");
    }});

    final SinkRecord transformedRecord = xform.apply(recordWithoutSchema);
    assertNull("Tombstone record not inserted.", transformedRecord.value());
  }

  @Test
  public void insertTombstoneOnValueWithoutSchemaSimple() {
    xform.configure(new HashMap<String, Object>(){{
      put(Tombstoner.TOMBSTONER_FIELD, "$[?(@.field == 'DELETE')]");
    }});

    final SinkRecord transformedRecord = xform.apply(recordWithoutSchema);
    assertNull("Tombstone record not inserted.", transformedRecord.value());
  }
  @Test
  public void noInsertTombstoneOnValueWithoutSchema() {
    xform.configure(new HashMap<String, Object>(){{
      put(Tombstoner.TOMBSTONER_FIELD, "$.f1.[?(@.field == 'DELETE')]");
    }});

    final SinkRecord transformedRecord = xform.apply(recordWithoutSchema);
    assertEquals("No tombstone due to no value match.", recordWithoutSchema.value(), transformedRecord.value());
  }

  @Test
  public void noInsertTombstoneOnInvalidPathValue() {
    xform.configure(new HashMap<String, Object>(){{
      put(Tombstoner.TOMBSTONER_FIELD, "$.field.[?(@.field == 'DELETE')]");
    }});

    final SinkRecord transformedRecord = xform.apply(recordWithoutSchema);
    assertEquals("Tombstone record inserted incorrectly on invalid value path.", recordWithoutSchema.value(), transformedRecord.value());
  }

  @Test
  public void insertTombstoneOnKeyWithSchema() {
    xformKey.configure(new HashMap<String, Object>(){{
      put(Tombstoner.TOMBSTONER_FIELD, "$.f1[?(@.field.f4 == 'DELETE')]");
    }});

    final SinkRecord transformedRecord = xformKey.apply(recordWithSchema);
    assertNull("Tombstone record not inserted.", transformedRecord.value());
  }

  @Test
  public void noInsertTombstoneOnKeyWithSchema() {
    xformKey.configure(new HashMap<String, Object>(){{
      put(Tombstoner.TOMBSTONER_FIELD, "$.f1.[?(@.field == 'DELETE')]");
    }});

    final SinkRecord transformedRecord = xformKey.apply(recordWithSchema);
    assertEquals("Tombstone record inserted incorrectly.", recordWithSchema.value(), transformedRecord.value());
  }

  @Test
  public void noInsertTombstoneOnInvalidPathKeySchema() {
    xformKey.configure(new HashMap<String, Object>(){{
      put(Tombstoner.TOMBSTONER_FIELD, "$.field.[?(@.field == 'DELETE')]");
    }});

    final SinkRecord transformedRecord = xformKey.apply(recordWithSchema);
    assertEquals("Tombstone record inserted incorrectly on invalid key path.", recordWithSchema.value(), transformedRecord.value());
  }

  @Test
  public void insertTombstoneOnValueWithSchema() {
    xform.configure(new HashMap<String, Object>(){{
      put(Tombstoner.TOMBSTONER_FIELD, "$.f1[?(@.field.f3 == 'DELETE')]");
    }});

    final SinkRecord transformedRecord = xform.apply(recordWithSchema);
    assertNull("Tombstone record not inserted.", transformedRecord.value());
  }
  @Test
  public void insertTombstoneOnValueWithSchemaSimple() {
    xform.configure(new HashMap<String, Object>(){{
      put(Tombstoner.TOMBSTONER_FIELD, "$[?(@.field == 'DELETE')]");
    }});

    final SinkRecord transformedRecord = xform.apply(recordWithSchema);
    assertNull("Tombstone record not inserted.", transformedRecord.value());
  }

  @Test
  public void noInsertTombstoneOnValueWithSchema() {
    xform.configure(new HashMap<String, Object>(){{
      put(Tombstoner.TOMBSTONER_FIELD, "$.f1.[?(@.field == 'DELETE')]");
    }});

    final SinkRecord transformedRecord = xform.apply(recordWithSchema);
    assertEquals("No tombstone due to no value match.", recordWithSchema.value(), transformedRecord.value());
  }

  @Test
  public void noInsertTombstoneOnInvalidPathValueSchema() {
    xform.configure(new HashMap<String, Object>(){{
      put(Tombstoner.TOMBSTONER_FIELD, "$.field.[?(@.field == 'DELETE')]");
    }});

    final SinkRecord transformedRecord = xform.apply(recordWithSchema);
    assertEquals("Tombstone record inserted incorrectly on invalid value path.", recordWithSchema.value(), transformedRecord.value());
  }
}