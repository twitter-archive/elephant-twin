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
package com.twitter.elephanttwin.lucene.indexing;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * <p>A "document" used to represent a Hadoop Split, which can be
 * indexed by an arbitrary number of fields. The idea is that in
 * cases when some "field" in a file has extremely high cardinality --
 * say, IP addresses in web logs of a busy website -- you can "index"
 * the files based on values of these fields in a given Hadoop file split,
 * and only scan relevant splits when you are looking for a small subset of
 * all possible values of the field.</p>
 *
 * This is a cheater's block-level index (find all the necessary blocks via a query
 */
public class HadoopSplitDocument implements Writable {

  private FileSplit split;

  // Negative values here indicate "bad" state. Serializing a SplitDoc in a bad
  // state will cause an exception.

  // offset means offset inside the Split. We might create several HadoopSplitDocs per single
  // split, in order to make small docs that fit in memory for Lucene.
  private long offset = -1;
  private int cnt = -1;

  // Each element is a set of values for a given key.
  private Map<String, Set<String>> keyValueListMap = Maps.newHashMap();

  public HadoopSplitDocument() {}

  public HadoopSplitDocument(FileSplit split, long offset, int cnt) {
    this.split = split;
    this.offset = offset;
    this.cnt = cnt;
  }

  public void clear() {
    keyValueListMap.clear();
    offset = -1;
    cnt = -1;
  }

  public Map<String, Set<String>> getKeyValueListMap() {
    return keyValueListMap;
  }

  public void setKeyValueListMap(Map<String, Set<String>> keyValueListMap) {
    this.keyValueListMap = keyValueListMap;
  }

  public void addKeyValue(String key, String value) {
    Set<String> set = keyValueListMap.get(key);
    if (set == null) {
      set = Sets.newHashSet();
    }
    set.add(value);
    keyValueListMap.put(key, set);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    clear();
    cnt = in.readInt();
    offset = in.readLong();
    int numKeyVals = in.readInt();
    keyValueListMap = Maps.newHashMapWithExpectedSize(numKeyVals);
    for (int i = 0; i < numKeyVals; i++) {
      String key = in.readUTF();
      int numEntries = in.readInt();
      Set<String> valueSet = Sets.newHashSetWithExpectedSize(numEntries);
      for (int j = 0; j < numEntries; j++) {
        valueSet.add(in.readUTF());
      }
      keyValueListMap.put(key, valueSet);
    }
    if (split == null) {
      // Initializing to nonsense
      split = new FileSplit(null, 0, 0, null);
    }
    split.readFields(in);

  }

  /*
   * Format is as follows:
   * cnt, offset, numKeys, [key, valueType, [values]], split
   */
  @Override
  public void write(DataOutput out) throws IOException {
    Preconditions.checkState(cnt >= 0, "cnt must be gte than 0. SplitDoc not constructed correctly.");
    Preconditions.checkState(offset >= 0, "offset must be gte 0. SplitDoc not constructed correctly.");
    Preconditions.checkNotNull(keyValueListMap, "keyValueList must not be null. SplitDoc not constructed correctly.");
    Preconditions.checkNotNull(split, "FileSplit must not be null. SplitDoc not constructed correctly.");

    out.writeInt(cnt);
    out.writeLong(offset);
    out.writeInt(keyValueListMap.size());
    for (Map.Entry<String, Set<String>> entry : keyValueListMap.entrySet()) {
      out.writeUTF(entry.getKey());
      Set<String> values = entry.getValue();
      out.writeInt(values.size());
      for (String s : values) {
        out.writeUTF(s);
      }
    }
    split.write(out);
  }

  public FileSplit getSplit() {
    return split;
  }

  public void setSplit(FileSplit split) {
    this.split = split;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public int getCnt() {
    return cnt;
  }

  public void setCnt(int cnt) {
    this.cnt = cnt;
  }


}
