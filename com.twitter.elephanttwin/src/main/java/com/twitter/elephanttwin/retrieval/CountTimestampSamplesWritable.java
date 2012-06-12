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
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.Writable;
/**
 * @author Alex Levenson
 */
public class CountTimestampSamplesWritable implements Writable {
  private CountTimestampWritable countTimestampWritable;
  private List<Long> samples;
  public CountTimestampSamplesWritable(long count, long epochMilliseconds, List<Long> samples) {
    this.countTimestampWritable = new CountTimestampWritable(count, epochMilliseconds);
    this.samples = samples;
  }
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    countTimestampWritable.write(dataOutput);
    dataOutput.writeInt(samples.size());
    for (long sample : samples) {
      dataOutput.writeLong(sample);
    }
  }
  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.countTimestampWritable = new CountTimestampWritable(0, 0);
    this.countTimestampWritable.readFields(dataInput);
    int numSamples = dataInput.readInt();
    samples = Lists.newLinkedList();
    for(int i=0; i<numSamples; i++) {
      samples.add(dataInput.readLong());
    }
  }
  public List<Long> getSamples() {
    return samples;
  }
  public void setSamples(List<Long> samples) {
    this.samples = samples;
  }
  public void setCount(long count) {
    countTimestampWritable.setCount(count);
  }
  public long getEpochMilliseconds() {
    return countTimestampWritable.getEpochMilliseconds();
  }
  public long getCount() {
    return countTimestampWritable.getCount();
  }
  public void setEpochMilliseconds(long epochMilliseconds) {
    countTimestampWritable.setEpochMilliseconds(epochMilliseconds);
  }
}