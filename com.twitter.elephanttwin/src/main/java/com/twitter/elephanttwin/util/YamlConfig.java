/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.elephanttwin.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;
import com.esotericsoftware.yamlbeans.YamlWriter;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


/**
 * Configuration Map that can have nested sub elements and which can be initialized from
 * various forms of Yaml sources.
 *
 * @author Dmitriy Ryaboy
 */
public class YamlConfig {
  public static final Logger LOG = LogManager.getLogger(YamlConfig.class);

  /**
   * This is used for pattern matching in the transform method when a key is passed that is meant
   * to be a reference to a list item transform (i.e. foo.bar[2])
   * @see YamlConfig.transform(YamlConfig)
   **/
  private static final Pattern PATTERN = Pattern.compile("(.+)\\[(\\d+|APPEND)\\]");

  /**
   * Reference Type
   */
  public static enum RefType {
    SCALAR, MAP, LIST;
  }

  protected final Map<String, Object> config;

  public YamlConfig() {
    this(null);
  }

  /**
   * Builds a new YamlConfig object from the items in the map.
   * @param config
   */
  public YamlConfig(Map<String, Object> config) {
    this.config = config == null ? new HashMap<String, Object>() : config;
  }

  /**
   * Loads a new YamlConfig from the given Reader
   * @param rdr
   * @return
   */
  public static YamlConfig load(Reader rdr) {
    YamlReader reader = new YamlReader(rdr);
    return loadFromReader(reader);
  }

  /**
   * Loads a new YamlConfig from the given InputStream
   * @param in
   * @return
   */
  public static YamlConfig load(InputStream in) {
    YamlReader reader = new YamlReader(new InputStreamReader(in));
    return loadFromReader(reader);
  }

  /**
   * Loads a new YamlConfig from the given String
   * @param yaml
   * @return
   */
  public static YamlConfig load(String yaml) {
    YamlReader reader = new YamlReader(yaml);
    return loadFromReader(reader);
  }

  /**
   * Returns a new YamlConfig loaded from the given YamlReader
   * @param reader
   * @return
   */
  @SuppressWarnings("unchecked")
  private static YamlConfig loadFromReader(YamlReader reader) {
    try {
      return new YamlConfig(reader.read(Map.class));
    } catch (YamlException e) {
      LOG.error("Could not read config", e);
      return null;
    }
  }

  /**
   * Loads the YamlConfig from the given file
   * @param filename
   * @return
   */
  public static YamlConfig loadFile(String filename) {
    FileInputStream istream = null;
    try {
      istream = new FileInputStream(filename);
      return load(istream);
    } catch (FileNotFoundException e) {
      LOG.error("Failed loading yaml file " + filename, e);
      return null;
    } finally {
      if (istream != null) {
        try {
          istream.close();
        } catch (IOException e) {
          //
        }
      }
    }
  }

  /**
   * Transforms the fields from <code>fromConf</code> into this object. Transforming fields is done
   * by iterating over all of the top-level key names passed and overridding each. See
   * <code>transform(String key, Object value)</code>.
   * @param transformPlan The configs to transform the YamlConfig with
   * @throws IllegalArgumentException if a key doesn't already exist
   */
  public void transform(YamlConfig transformPlan) {

    //for each top-level overriden param
    for (String key : transformPlan.getRawConfig().keySet()) {
      transform(key, transformPlan.get(key));
    }
  }

  /**
   * Merges <code>value</code> into the <code>YamlConfig</code> object at location <code>key</code>.
   * The transform key supports a dot-notation to override any key value at any depth of the
   * YamlConfig. For example:
   * <ul>
   * <li><i>foo</i> Override the top-level <i>foo</i> value with the contents of <i>foo</i></li>
   * <li><i>foo.bar.bat</i> Override the node at <i>foo.bar.bat</i> the assigned value</li>
   * <li><i>foo.bar[2].bat</i> Override the value of <i>bat</i> in the 3rd item of the list, or add
   * a 3rd element if the list is of size 2.
   * <li><i>foo.bar[APPEND].bat</i> Appends <i>bat</i> at the end of the list.
   * </ul>
   * @param key The key of the configs to transform
   * @param value The config object to merge in.
   * @throws IllegalArgumentException if a param doesn't already exist
   */
  @SuppressWarnings("unchecked")
  public void transform(String key, Object value) {
    Preconditions.checkNotNull(key, "Can not call transform with a null key");

    //for key aa.bb.cc, transpose into a deque of:
    // aa
    // bb
    // cc
    Deque<String> keyTokensDeque = new LinkedList<String>();
    for (String token : key.split("\\.")) {
      keyTokensDeque.addLast(token);
    }

    recursiveTransform(this, keyTokensDeque, key, value);
  }

  /**
   * Pops a token from keyTokensDeque and if we're at a leaf, adds the value to the config.
   * Otherwise, pull out the referenced YamlConfig and recurse one level deeper to the next node.
   */
  @SuppressWarnings("unchecked")
  private void recursiveTransform(YamlConfig thisConfig, Deque<String> keyTokensDeque,
                                  String key, Object value) {
    Preconditions.checkState(keyTokensDeque.size() > 0,
      "Trying to pop more key tokens than exist for " + key);

    String token = keyTokensDeque.pop();
    boolean atLeaf = keyTokensDeque.size() == 0;

    // tokens support a dot notation to insert items at arbitrary locations in the config tree.
    // traverse down each level of the branch passed until we find the config to override
    Matcher m = PATTERN.matcher(token);

    // matches if we're overriding an item in a list, i.e., foo[1]
    if (m.matches()) {
      String targetSection = m.group(1);

      // First, make sure we have list to add to
      List listObject = (List) thisConfig.get(targetSection);
      if (listObject == null) {
        throw new IllegalArgumentException("YAML list does not exist for key: " + key);
      }

      int itemIndex =
        "APPEND".equals(m.group(2)) ? listObject.size() : Integer.parseInt(m.group(2));

      // then we have to determine if we're dealing with a list of Strings or a List of YamlConfigs.
      // if we're at a leaf and the value is a string, then it should a List of Strings.
      if (atLeaf) {
        List list = (value instanceof String)
          ? thisConfig.getList(targetSection) : (List) thisConfig.getRawConfig().get(targetSection);

        // are we appending a new item to the list or updating an existing item
        if (itemIndex == list.size()) {
          list.add(value);
        } else if (itemIndex > list.size()) {
          throw new IllegalArgumentException("Transform: trying to insert at index " + itemIndex
            + " into a list of size " + list.size() + ". Transform key=" + key);
        } else {
          list.set(itemIndex, value);
        }

        return; //traversal complete

      } else {
        List<Map<String, Object>> list =
          (List<Map<String, Object>>) thisConfig.getRawConfig().get(targetSection);

        // are we appending a new YamlConfig to the list or modifying an existing one?
        YamlConfig listItemYaml;
        if (itemIndex == list.size()) {
          listItemYaml = YamlConfig.load("");
          list.add(listItemYaml.getRawConfig());
        } else if (itemIndex > list.size()) {
          throw new IllegalArgumentException("YAML map config list index out of range: " + key);
        } else {
          listItemYaml = new YamlConfig(list.get(itemIndex));
        }

        recursiveTransform(listItemYaml, keyTokensDeque, key, value);
      }

    } else {
      // if at the leaf node, update config and we're done
      if (atLeaf) {
        thisConfig.getRawConfig().put(token, value);
        return; // traversal complete
      }

      // adding a new node if one does not exist
      if (thisConfig.getNestedConfig(token) == null) {
        thisConfig.getRawConfig().put(token, new HashMap<String, Object>());
      }

      recursiveTransform(thisConfig.getNestedConfig(token), keyTokensDeque, key, value);
    }
  }

  public static YamlConfig loadResource(String resource) {
    return load(YamlConfig.class.getResourceAsStream(resource));
  }

  /**
   * Returns the RefType item associated with key if present or null
   * @param key
   * @return
   */
  public RefType getRefType(String key) {
    RefType result = null;
    if (config.containsKey(key)) {
      Object obj = config.get(key);
      if (obj instanceof List<?>) {
        result = RefType.LIST;
      } else if (obj instanceof Map<?, ?>) {
        result = RefType.MAP;
      } else {
        result = RefType.SCALAR;
      }
    }
    return result;
  }

  public Object get(String key) {
    return config.get(key);
  }

  /**
   * When we have a key without a value like:
   *
   * <p> key:
   *
   * <p>The value is an empty string.
   *
   * @param key
   * @return true if the key has no contents or does not exist.
   */
  public boolean isEmptyKey(String key) {
    if (config.containsKey(key)) {
      if (config.get(key).getClass() == String.class
          && ((String) config.get(key)).length() == 0) {
        return true;
      }
      return false;
    }
    return true;
  }

  /**
   * Returns a String config element if present or a default value
   * @param key
   * @param defaultValue
   * @return
   */
  public String getString(String key, String defaultValue) {
    if (config.containsKey(key)) {
      return (String) config.get(key);
    } else {
      return defaultValue;
    }
  }

  /**
   * Retrieves the String value associated with key from the config.
   * @param key
   * @return
   */
  public String getString(String key) {
    return getString(key, null);
  }

  /**
   * Retrieves the required String value associated with key from the
   * config, with the pre-condition that it is not null.
   * @param key
   * @return value associated with key as String
   */
  public String getRequiredString(String key) {
    Preconditions.checkState(config.containsKey(key), key + " is not set!");
    return (String) config.get(key);
  }

  /**
   * Returns an Integer config element if present or a default value
   * @param key
   * @param defaultValue
   * @return
   */
  public Integer getInt(String key, Integer defaultValue) {
    if (config.containsKey(key)) {
      Object val = config.get(key);
      return val instanceof Number ? ((Number) val).intValue() : Integer.parseInt((String) val);
    } else {
      return defaultValue;
    }
  }

  public Integer getInt(String key) {
    return getInt(key, null);
  }

  /**
   * Returns a Boolean config element if present or a default value
   * @param key
   * @param defaultValue
   * @return
   */
  public Boolean getBoolean(String key, Boolean defaultValue) {
    if (config.containsKey(key)) {
      Object val = config.get(key);
      return val instanceof Boolean ? (Boolean) val : Boolean.valueOf((String) val);
    } else {
      return defaultValue;
    }
  }

  public Boolean getBoolean(String key) {
    return getBoolean(key, null);
  }

  /**
   * Returns a Long config element if present or a default value
   * @param key
   * @param defaultValue
   * @return
   */
  public Long getLong(String key, Long defaultValue) {
    if (config.containsKey(key)) {
      Object val = config.get(key);
      return val instanceof Number ? ((Number) val).longValue() : Long.parseLong((String) val);
    } else {
      return defaultValue;
    }
  }

  public Long getLong(String key) {
    return getLong(key, null);
  }

  /**
   * Returns a list of config elements associated with key with an optional default value is key is
   * not found
   * @param key
   * @param defaultValue
   * @return
   */
  @SuppressWarnings("unchecked")
  public List<String> getList(String key, List<String> defaultValue) {
    if (config.containsKey(key)) {
      return (List<String>) config.get(key);
    } else {
      return defaultValue;
    }
  }

  /**
   * Returns a List of config elements associated with key
   * @param key
   * @return
   */
  public List<String> getList(String key) {
    return getList(key, null);
  }

  /**
   * Returns a nested List of YamlConfig objects or null
   * @param key
   * @return
   */
  @SuppressWarnings("unchecked")
  public List<YamlConfig> getNestedList(String key) {
    if (config.containsKey(key)) {
      List<Map<String, Object>> list = (List<Map<String, Object>>) config.get(key);
      return Lists.transform(list, new Function<Map<String, Object>, YamlConfig>() {
        @Override
        public YamlConfig apply(Map<String, Object> m) {
          return new YamlConfig(m);
        }
      });
    } else {
      return null;
    }
  }

  /**
   * Returns nested YamlConfig object for key, or a new, empty one.
   * @param key
   * @return
   */
  public YamlConfig getNestedOrEmptyConfig(String key) {
    YamlConfig nestedConfig = getNestedConfig(key);
    if (nestedConfig == null) {
      nestedConfig = new YamlConfig(new HashMap<String, Object>());
    }
    return nestedConfig;
  }

  /**
   * Returns nested YamlConfig object for key, or null
   * @param key
   * @return
   */
  @SuppressWarnings("unchecked")
  public YamlConfig getNestedConfig(String key) {
    if (config.containsKey(key)) {
      return new YamlConfig((Map<String, Object>) config.get(key));
    } else {
      return null;
    }
  }

  /**
   * Returns a map of configuration key value pairs, crated from a nested YamlConfig object.
   * @param key
   * @param defaultValue used when no nested config is found for key
   * @return
   */
  public Map<String, String> getNestedStringMap(String key, Map<String, String> defaultValue) {
    YamlConfig nestedConfig = getNestedConfig(key);
    if (nestedConfig != null) {
      Map<String, String> map = Maps.newHashMap();
      for (String subKey : nestedConfig.getKeys()) {
        map.put(subKey, nestedConfig.getString(subKey));
      }
      return map;
    } else {
      return defaultValue;
    }
  }

  public Map<String, String> getNestedStringMap(String key) {
    return getNestedStringMap(key, null);
  }

  public Set<String> getKeys() {
    return config.keySet();
  }

  /**
   * Does this config contain the given key?
   * @param key config key
   * @return
   */
  public boolean containsKey(String key) {
    return config.containsKey(key);
  }

  /**
   * Because this little wrapper class doesn't implement everything...
   */
  public Map<String, Object> getRawConfig() {
    return config;
  }

  @Override
  public String toString() {
    StringWriter stringWriter = new StringWriter();
    YamlWriter writer = new YamlWriter(stringWriter);
    try {
      writer.write(config);
      writer.close();
    } catch (YamlException e) {
      LOG.error(e);
      return "";
    }
    return stringWriter.toString();

  }

  /**
   * Dirty test main
   * @param args argument array
   */
  public static void main(String[] args) {
    YamlConfig conf = null;
    if (args.length > 0) {
      System.err.println("reading from file " + args[0]);
      conf = YamlConfig.loadFile(args[0]);
    } else {
      System.err.println("reading from stdin");
      conf = YamlConfig.load(System.in);
    }
    if (conf != null) {
      System.out.println(conf.toString());
    } else {
      System.err.println("Failed to load the config.");
    }
  }
}
