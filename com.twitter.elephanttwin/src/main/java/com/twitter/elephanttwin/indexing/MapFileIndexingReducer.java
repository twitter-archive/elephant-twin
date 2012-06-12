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
package com.twitter.elephanttwin.indexing;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.twitter.elephanttwin.io.ListLongPair;
import com.twitter.elephanttwin.io.LongPairWritable;
import com.twitter.elephanttwin.io.TextLongPairWritable;

/**
 * The reducer simply gets all indexed block offsets for the same text (key
 * value) and put them together as a list to be written to index files.
 */

public class MapFileIndexingReducer extends
Reducer<TextLongPairWritable, LongPairWritable, Text, ListLongPair> {
  private static final Logger LOG = Logger
      .getLogger(MapFileIndexingReducer.class);
  private long maxBlockSize;
  private long combinedCnt = 0; //how many blocks are combined.

  //how many overlapping cases seen where combination cannot be done
  //due to max block size limititation.
  private long removedOverlapping = 0;



  @Override
  protected void setup(Context context
      ) throws IOException, InterruptedException {
    maxBlockSize = context.getConfiguration().getLong("dfs.block.size", 128*1024*1024);
  }

  @Override
  public void reduce(TextLongPairWritable keypair, Iterable<LongPairWritable> values,
      Context context) throws IOException, InterruptedException {
    /**
     * If Mapper 1 produces <a,b> for key1
     * and Mapper 2 produces <c,d> for key1 and c < b,
     * a      b
     *    c       d
     *  we remove the overlapping by write
     *    <a,c> <c,d> to the index file
     *    or <a,d> to the index file  if d - a is less than the HDFS block
     *    size limit.
     *    The overlapping case can happen for lzo binary block reader when a
     *    binary block span across two lzo blocks. One of the lzo block offset
     *    is [c,b].
     *    Mapper 1 may combine other blocks for key 1 and produces <a,b>,
     *    Mapper 2 may combine other blocks for key 1 and produces <c,d>.
     */

    ListLongPair indexes = new ListLongPair();
    Iterator<LongPairWritable> it = values.iterator();
    LongPairWritable current = null;
    LongPairWritable previous = null;
    if(it.hasNext())
       previous = new LongPairWritable(it.next());
    while(it.hasNext()) {
      current = it.next();
      if(current.getFirst() <= previous.getSecond() &&
          current.getFirst() >= previous.getFirst()) {  //overloapping case

        if(current.getSecond() - previous.getFirst() < maxBlockSize){
          //combine both entries to one
          previous.setSecond(current.getSecond());
          combinedCnt++;
        }
        else { //cannot combine, remove the overlapping
          removedOverlapping++;
          if(previous.getFirst() < current.getFirst()) {
            //don't add <a,a> to the result
            previous.setSecond(current.getFirst());
            indexes.add(previous);
          }
          previous.set(current);
        }
      }
      else {//no overlapping, write the last entry,  remember the current one
        indexes.add(previous);
        previous.set(current);
      }
    }

    if(previous != null)
      indexes.add(previous);

    context.write(keypair.getText(), indexes);
  }

@Override
  protected void cleanup(Context context
                         ) throws IOException, InterruptedException {
    LOG.info("number of combined overlapping blocks: " + combinedCnt);
    LOG.info("number of overlapping blocks not combined: " + removedOverlapping);
  }
}