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

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;

import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;


public abstract class Tombstoner<R extends ConnectRecord<R>> implements Transformation<R> {
  public static final String TOMBSTONER_FIELD = "tombstoner.condition";

  public static final String OVERVIEW_DOC =
    "Create Tobstone record based on the input criteria";

  private static final Validator Validate_JSONPath = (name, value) -> {
    try {
      JsonPath.compile((String) value);
    } catch (InvalidPathException e) {
      throw new ConfigException(name, value, "Invalid JSON path " + e.getMessage() + ".");
    } catch (IllegalArgumentException e) {
      throw new ConfigException(name, value, "Null or empty value not allowed " + e.getMessage() + ".");
    }
  };

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
    .define(TOMBSTONER_FIELD, ConfigDef.Type.STRING, 
            ConfigDef.NO_DEFAULT_VALUE, Validate_JSONPath, 
            ConfigDef.Importance.HIGH,
      "JSONPath of the field name for Tombstone detection");

  private static final String PURPOSE_NO_SCHEMA = "creating tombstone record with no schema";
  private static final String PURPOSE_WITH_SCHEMA = "creating tombstone record with schema";

  private JsonPath fieldName;

  @Override
  public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    fieldName =  JsonPath.compile(config.getString(TOMBSTONER_FIELD));
  }

  @Override
  public R apply(R record) {
    if (operatingValue(record) != null && isTombstone(record)) {
      return record.newRecord(
          record.topic(),
          record.kafkaPartition(),
          record.keySchema(),
          record.key(),
          null,
          null,
          record.timestamp()
      );    
    } else {
      return record;
    }
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
  }

  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  public static class Key<R extends ConnectRecord<R>> extends Tombstoner<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.keySchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.key();
    }

  }

  public static class Value<R extends ConnectRecord<R>> extends Tombstoner<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }

  }

  private boolean isTombstone(R record) {
    // check if this is record with schema
    final Schema schema = operatingSchema(record);
    Object valueMap = (schema == null) ? 
        requireMap(operatingValue(record), PURPOSE_NO_SCHEMA) : 
        extractMapFromStruct(requireStruct(operatingValue(record), PURPOSE_WITH_SCHEMA),
        schema);
    try {
      List<?> tombstoneField = fieldName.read(valueMap, 
                        Configuration.defaultConfiguration().addOptions(Option.ALWAYS_RETURN_LIST));
    if (tombstoneField.size() != 0)
      return true;
      // ignore exceptions in spec or search
    } catch (PathNotFoundException e) {
    } catch (InvalidPathException e) {
    }
  return false;
  }

  @SuppressWarnings("unchecked")
  private Object extractMapFromStruct(Object value, Schema schema)
  {
    if (schema.type().isPrimitive())
      return value;
    switch (schema.type()) {
      case STRUCT:{
        HashMap<String, Object> valueStruct = new HashMap<>();
        for (Field field : ((Struct) value).schema().fields()) {
          Object tmpVal = extractMapFromStruct(((Struct)value).get(field.name()), field.schema());
          if (tmpVal != null)
            valueStruct.put(field.name(), tmpVal);
        }
        return valueStruct;
      }
      case MAP: {
        HashMap<Object, Object> valueMap = new HashMap<>();
        List<Map<String, Object>> valueList = new ArrayList<>();
        Schema keySchema = schema.keySchema();
        Schema valueSchema = schema.valueSchema();
    
    
        boolean isMap = keySchema.type() == Schema.Type.STRING;
    
        for (Map.Entry<Object, Object> field : ((Map<Object,Object>) value).entrySet()) {
          Map<String, Object> entry = new HashMap<>();
          Object tmpKey = extractMapFromStruct(
                  field.getKey(),
                  keySchema
          );
          Object tmpValue = extractMapFromStruct(
                  field.getValue(),
                  valueSchema
          );
    
          if (isMap) {
            valueMap.put(tmpKey, tmpValue);
          } else {
            entry.put("key", tmpKey);
            entry.put("value", tmpValue);
            valueList.add(entry);
          }
        }
        return isMap ? valueMap : valueList;
      }
      case ARRAY: {
        Schema valueSchema = schema.valueSchema();
        List<Object> valueList = new ArrayList<>();
        for (Object field : (List<Object>) value) {
          Object tmpValue = extractMapFromStruct(field, valueSchema);
          valueList.add(tmpValue);
        }
        return valueList;
      }
      default:
        // check if we need more
        return value;
    }
  }
}


