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
package com.twitter.elephanttwin.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.io.Writable;


/**
 * A Writable class to write a list of LongWritable values.
 */
public class ListLongPair implements Writable {

  private List<LongPairWritable> list;

  public ListLongPair() {
    list = new ArrayList<LongPairWritable>();
  }

  public ListLongPair(List<LongPairWritable> l) {
    list = l;
  }

  public ListLongPair(int initialCapacity) {
    list = new ArrayList<LongPairWritable>(initialCapacity);
  }

  public int size() {
    return list.size();
  }

  public void add(LongPairWritable value) {
    LongPairWritable a = new LongPairWritable(value);
    list.add(a);
  }

  public void add(ListLongPair l) {
    list.addAll(l.get());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(list.size());
    Iterator<LongPairWritable> itr = list.iterator();

    while (itr.hasNext()) {
      LongPairWritable element = itr.next();
      element.write(out);
    }
  }

  @Override
  public String toString() {
    Iterator<LongPairWritable> it = list.listIterator();
    StringBuilder str = new StringBuilder();
    while (it.hasNext()) {
      str.append(" " + it.next());
    }
    return str.toString();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numElements = in.readInt();
    list = new ArrayList<LongPairWritable>(numElements);

    for (int cnt = 0; cnt < numElements; cnt++) {
      LongPairWritable l = new LongPairWritable();
      l.readFields(in);
      list.add(l);
    }
  }

  public List<LongPairWritable> get() {
    return list;
  }
}