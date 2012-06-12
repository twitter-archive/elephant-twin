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
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.twitter.elephanttwin.io.LongPairWritable;
import com.twitter.elephanttwin.retrieval.Expression.BinaryExpression;

/**
 * IndexedFilterRecordReader works on a FileSplit or IndexedFileSplit.
 * In the latter case, it goes through a chain of (merged) subblocks,
 * uses FilterRecordReader to do the actual scan and filtering.
 */
public class IndexedFilterRecordReader<K, V> extends RecordReader<K, V> {

  private FilterRecordReader<K, V> filterReader;
  private int subBlockCnt = 0; // total (sub)blocks assigned to this split;
  private int currentSubBlock; // the i th block being read;
  private TaskAttemptContext context;
  private List<InputSplit> fileSplits;
  private BinaryExpression filter;
  HashMap<String, Method> columnName2Method;


  public IndexedFilterRecordReader(
      BinaryExpression filter,
      HashMap<String, Method> columnName2Method) {
    this.filter = filter;
    this.columnName2Method = columnName2Method;
    filterReader = new FilterRecordReader<K, V>(filter, columnName2Method);
  }



  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {

    this.context = context;
    if (split.getLength() <= 0) {
      filterReader = null;
      return;
    }

    fileSplits = computeFileSplits(split, context);
    subBlockCnt = fileSplits.size();

    if (subBlockCnt == 0) {
      filterReader = null;
      return;
    }
    currentSubBlock = 1;
    filterReader.initialize(fileSplits.remove(0), context);
  }

  private List<InputSplit> computeFileSplits(InputSplit split,
      TaskAttemptContext context) throws IOException {
    List<InputSplit> fileSplits = new LinkedList<InputSplit>();
    if (split instanceof IndexedFileSplit) {
      IndexedFileSplit indexedSplits = (IndexedFileSplit) split;

      Path path = indexedSplits.getPath();
      List<LongPairWritable> indexedBlocks = indexedSplits.getIndexedBlocks();
      for (LongPairWritable pair : indexedBlocks) {
        long start = pair.getFirst();
        long end = pair.getSecond();
        long length = end - start;
        FileSplit fileSplit = new FileSplit(path, start, length, indexedSplits.getLocations());
        fileSplits.add(fileSplit);
      }
    } else {
      fileSplits.add(split);
    }
    return fileSplits;
  }

  /*
   * if we finish reading one subblock, move to the next subblock, until all
   * subblocks assigned in this IndexedFileSplit are read
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {

    if (filterReader == null) {
      return false;
    }

    if (filterReader.nextKeyValue()) {
      return true;
    }

    while (true) {
      //close the previous RecordReader.
      if (filterReader != null) {
        filterReader.close();
      }
      if (currentSubBlock == subBlockCnt) {
        return false;
      } else {
        currentSubBlock++;
        // initialize the next filterReader;
        filterReader = new FilterRecordReader<K, V>(filter, columnName2Method);
        filterReader.initialize(fileSplits.remove(0), context);
        if ( filterReader.nextKeyValue()) {
          return true;
        }
      }
    }
  }

  @Override
  public K getCurrentKey() throws IOException, InterruptedException {
    return filterReader.getCurrentKey();
  }

  @Override
  public V getCurrentValue() throws IOException, InterruptedException {
    return filterReader.getCurrentValue();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (subBlockCnt > 0) {
      return Math.min(1f, (float)currentSubBlock / subBlockCnt);
    }
    else {
      return 1;
    }
  }

  @Override
  public void close() throws IOException {
    if (filterReader != null) {
      filterReader.close();
    }
  }
}