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
import org.apache.hadoop.io.WritableComparable;

public class LongPairWritable implements WritableComparable<LongPairWritable> {

  long first;
  long second;

  public LongPairWritable() {
  }

  public LongPairWritable(long s, long e) {
    first = s;
    second = e;
  }

  public LongPairWritable(LongWritable s, LongWritable e) {
    first = s.get();
    second = e.get();
  }

  public LongPairWritable(LongPairWritable value) {
    first = value.getFirst();
    second = value.getSecond();
  }

  public void setFirst(long s) {
    first = s;
    ;
  }

  public void setSecond(long e) {
    second = e;
  }

  public long getFirst() {
    return first;
  }

  public long getSecond() {
    return second;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(first);
    out.writeLong(second);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    first = in.readLong();
    second = in.readLong();
  }

  @Override
  public int compareTo(LongPairWritable p) {
    int result = Long.signum(first - p.getFirst());
    return (result != 0) ? result : Long.signum(second - p.getSecond());
  }

  @Override
  public String toString() {
    return first + "," + second;
  }

  @Override
  public int hashCode() {
    return (int) (first ^ (first >>> 32));
  }

  public boolean equals(LongPairWritable p) {
    return (first == p.getFirst());
  }

  /**
   * set this object's component values to be the same as  p's.
   * @param current
   */
  public void set(LongPairWritable p) {
   first = p.getFirst();
   second = p.getSecond();
  }
}