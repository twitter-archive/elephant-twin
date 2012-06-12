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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class TextLongPairWritable implements WritableComparable<TextLongPairWritable> {

  Text key;
  long value;

  public TextLongPairWritable() {
  }

  public TextLongPairWritable(Text t, long v) {
    key = t;
    value = v;
  }

  public TextLongPairWritable(Text t, LongWritable v) {
    key = t;
    value = v.get();
  }

  public void setText(String t1) {
    if (key == null)
      key = new Text();
    key.set(t1.getBytes());
  }

  public void setLong(long v) {
    value = v;
  }

  public void setLong(LongWritable v) {
    value = v.get();
  }

  public Text getText() {
    return key;
  }

  public long getLong() {
    return value;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(value);
    key.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    value = in.readLong();
    key = new Text();
    key.readFields(in);
  }

  @Override
  public int compareTo(TextLongPairWritable p) {

    int cmp = key.compareTo(p.getText());
    if (cmp != 0) {
      return cmp;
    }
    long l2 = p.getLong();
    return Long.signum(value - l2);
  }

  /**
   *
   * sort first on the text component of the class, then the number component of
   * the class
   *
   */
  public static class PairComparator extends WritableComparator {
    public PairComparator() {
      super(TextLongPairWritable.class, true);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public int compare(WritableComparable p1, WritableComparable p2) {
      return p1.compareTo(p2);
    }
  }

  /**
   *
   * Sorting is based only on text component of the class
   *
   */
  public static class KeyOnlyComparator extends WritableComparator {
    public KeyOnlyComparator() {
      super(TextLongPairWritable.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable p1, WritableComparable p2) {
      TextLongPairWritable k1 = (TextLongPairWritable) p1;
      TextLongPairWritable k2 = (TextLongPairWritable) p2;
      return k1.getText().compareTo(k2.getText());
    }
  }

  /**
   *
   * Partition based only on the first (text) part of the class
   *
   */
  public static class Parititioner extends Partitioner<TextLongPairWritable, Object> {

    @Override
    public int getPartition(TextLongPairWritable key, Object value, int numPartitions) {

      return (new HashPartitioner<Text, Object>()).getPartition(key.getText(),
          value, numPartitions);
    }
  }

  @Override
  public String toString() {
    return key + "," + value;
  }

  @Override
  public int hashCode() {
    return key.hashCode() + (int) value;
  }

  public boolean equals(TextLongPairWritable p) {
    return (key.equals(p.getText()));
  }

  public void setText(Text text) {
    key.set(text);
  }
}