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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.twitter.elephanttwin.io.ListLongPair;
import com.twitter.elephanttwin.io.LongPairWritable;

/**
 * IndexedFileSplit contains a list of sub-blocks to be read by a single
 * Mapper. Thus it can reduce the number of mappers to be used for the job.
 */
public class IndexedFileSplit extends FileSplit {
  private Path file;
  ListLongPair indexedBlocks;
  long first_start;
  long total_bytes;
  private String[] hosts;

  /*
   * workaround since FileSplit(){} is invisible
   */
  IndexedFileSplit() {
    this(null, null, null);
  }

  /**
   * Constructs a split with host information
   * @param file
   *          the file name
   * @param hosts
   *          the list of hosts containing the block, possibly empty
   * @param indexedBlocks
   *          the list of indexed blocks in file which contain searched value.
   */
  public IndexedFileSplit(Path file, ListLongPair indexedBlocks,
      String[] locations) {

    super(file, 0, 0, null);
    this.file = file;
    this.indexedBlocks = indexedBlocks;
    this.hosts = locations;

    if (indexedBlocks != null && indexedBlocks.get().size() > 0) {
      first_start = (indexedBlocks.get()).get(0).getFirst();
      List<LongPairWritable> blocks = indexedBlocks.get();
      for (LongPairWritable block : blocks) {
        total_bytes += block.getSecond() - block.getFirst();
      }
    } else {
      first_start = 0;
      total_bytes = 0;
    }
  }

  public List<LongPairWritable> getIndexedBlocks() {
    return indexedBlocks.get();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(first_start);
    out.writeLong(total_bytes);
    Text.writeString(out, file.toString());
    indexedBlocks.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    first_start = in.readLong();
    total_bytes = in.readLong();
    file = new Path(Text.readString(in));
    indexedBlocks = new ListLongPair();
    indexedBlocks.readFields(in);
  }

  @Override
  public String[] getLocations() throws IOException {
    if (this.hosts == null) {
      return new String[] {};
    } else {
      return this.hosts;
    }
  }

  @Override
  public Path getPath() {
    return file;
  }

  @Override
  public long getStart() {
    return first_start;
  }

  @Override
  public long getLength() {
    return total_bytes;
  }

  @Override
  public String toString() {
    return  "[start= " + first_start +
        ", total bytes=" + total_bytes  +
         ", #blocks=" +  indexedBlocks.size() + ":" +
        indexedBlocks.toString() +"]";
  }
}