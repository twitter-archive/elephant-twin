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
package com.twitter.elephanttwin.retrieval;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A writable that holds a count and a timestamp
 *
 * @author Alex Levenson
 */
public class CountTimestampWritable implements Writable {
  private long count;
  private long epochMilliseconds;

  /**
   * @param count - how many
   * @param epochMilliseconds - at what time, unix epoch millisecond timestamp
   */
  public CountTimestampWritable(long count, long epochMilliseconds) {
    this.count = count;
    this.epochMilliseconds = epochMilliseconds;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(count);
    out.writeLong(epochMilliseconds);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    count = in.readLong();
    epochMilliseconds = in.readLong();
  }

  public long getCount() {
    return count;
  }

  public void setCount(long count) {
    this.count = count;
  }

  public long getEpochMilliseconds() {
    return epochMilliseconds;
  }

  public void setEpochMilliseconds(long epochMilliseconds) {
    this.epochMilliseconds = epochMilliseconds;
  }
}
