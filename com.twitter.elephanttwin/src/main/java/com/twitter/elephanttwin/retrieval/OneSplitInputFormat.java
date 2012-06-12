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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * for testing/debuging use. It generates only one input split
 * constructed from the startoffset and endoffset.
 */
public abstract class OneSplitInputFormat<K,V>  extends BlockIndexedFileInputFormat<K, V> {
  private static String START="OneSplitInputFormat.startoffset";
  private static String END = "OneSplitInputFormat.endoffset";


  public static void setSplit(Job job, long startoffset, long endoffset) {
     job.getConfiguration().setLong(START, startoffset);
     job.getConfiguration().setLong(END, endoffset);
  }

  @Override
  public List<InputSplit> getSplits(JobContext job
      ) throws IOException {
    Configuration conf = job.getConfiguration();
    FileSplit split = (FileSplit) super.getSplits(job).get(0);

    List<InputSplit> lists = new ArrayList<InputSplit>();

    lists.add(new FileSplit(split.getPath(),
        conf.getLong(START, 0),
        conf.getLong(END,0) - conf.getLong(START, 0),
        split.getLocations()));
    return lists;
  }
}