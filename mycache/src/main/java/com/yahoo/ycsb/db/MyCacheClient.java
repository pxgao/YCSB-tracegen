/**
 * Copyright (c) 2014-2015 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
//import java.net.InetSocketAddress;
import java.text.MessageFormat;
//import java.util.ArrayList;
import java.util.*;

import java.io.*;

// We also use `net.spy.memcached.MemcachedClient`; it is not imported
// explicitly and referred to with its full path to avoid conflicts with the
// class of the same name in this file.

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import org.apache.log4j.Logger;
//import org.python.core.PyInstance;  
//import org.python.util.PythonInterpreter; 

//import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Concrete Memcached client implementation.
 */
public class MyCacheClient extends DB {
  private Map data = new HashMap();
  //private PythonInterpreter python = null;
  //private PyInstance client = null; 
  private PrintWriter trace = null;

  private final Logger logger = Logger.getLogger(getClass());
  protected static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public void init() throws DBException {
    try {
      Properties propsCL = getProperties();
      String tracefilepath = propsCL.getProperty("tracefilepath");
      System.out.println("trace path:" + tracefilepath);
      this.trace = new PrintWriter(tracefilepath, "UTF-8");
    } catch (Exception e) {
      System.out.println(e);
      throw new DBException(e);
    }
  }

  @Override
  public Status read(
      String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    this.trace.println("read|" + table + "|" + key);
    key = createQualifiedKey(table, key);
    try {
      Object document = data.get(key);
      if (document != null) {
        fromJson((String) document, fields, result);
      }
      return Status.OK;
    } catch (Exception e) {
      logger.error("Error encountered for key: " + key, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(
      String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result){
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(
      String table, String key0, HashMap<String, ByteIterator> values) {
    String key = createQualifiedKey(table, key0);
    try {
      String v = toJson(values);
      data.put(key, v);
      this.trace.println("update|" + table + "|" + key0 + "|" + v.length());
      return Status.OK;
    } catch (Exception e) {
      logger.error("Error updating value with key: " + key, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(
      String table, String key0, HashMap<String, ByteIterator> values) {
    String key = createQualifiedKey(table, key0);
    try {
      String v = toJson(values);
      data.put(key, v);
      this.trace.println("insert|" + table + "|" + key0 + "|" + v.length());
      return Status.OK;
    } catch (Exception e) {
      logger.error("Error inserting value", e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    this.trace.println("delete|" + table + "|" + key);
    key = createQualifiedKey(table, key);
    try {
      data.remove(key);
      return Status.OK;
    } catch (Exception e) {
      logger.error("Error deleting value", e);
      return Status.ERROR;
    }
  }

  @Override
  public void cleanup() throws DBException {
    this.trace.close();
  }

  protected static String createQualifiedKey(String table, String key) {
    return MessageFormat.format("{0}-{1}", table, key);
  }

  protected static void fromJson(
      String value, Set<String> fields,
      Map<String, ByteIterator> result) throws IOException {
    JsonNode json = MAPPER.readTree(value);
    boolean checkFields = fields != null && !fields.isEmpty();
    for (Iterator<Map.Entry<String, JsonNode>> jsonFields = json.getFields();
         jsonFields.hasNext();
         /* increment in loop body */) {
      Map.Entry<String, JsonNode> jsonField = jsonFields.next();
      String name = jsonField.getKey();
      if (checkFields && fields.contains(name)) {
        continue;
      }
      JsonNode jsonValue = jsonField.getValue();
      if (jsonValue != null && !jsonValue.isNull()) {
        result.put(name, new StringByteIterator(jsonValue.asText()));
      }
    }
  }

  protected static String toJson(Map<String, ByteIterator> values)
      throws IOException {
    ObjectNode node = MAPPER.createObjectNode();
    HashMap<String, String> stringMap = StringByteIterator.getStringMap(values);
    for (Map.Entry<String, String> pair : stringMap.entrySet()) {
      node.put(pair.getKey(), pair.getValue());
    }
    JsonFactory jsonFactory = new JsonFactory();
    Writer writer = new StringWriter();
    JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(writer);
    MAPPER.writeTree(jsonGenerator, node);
    return writer.toString();
  }

  protected static int getSize(Map<String, ByteIterator> values)
      throws IOException {
    int size = 0;
    //HashMap<String, String> stringMap = StringByteIterator.getStringMap(values);
    for (Map.Entry<String, ByteIterator> pair : values.entrySet()) {
      size += pair.getValue().bytesLeft();
    }
    return size;
  }
}
