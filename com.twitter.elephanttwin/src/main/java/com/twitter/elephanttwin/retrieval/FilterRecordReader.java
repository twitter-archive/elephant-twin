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

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import com.twitter.elephanttwin.retrieval.Expression.BinaryExpression;
import com.twitter.elephanttwin.retrieval.Expression.Column;
import com.twitter.elephanttwin.retrieval.Expression.Const;

/**
 * A "wrap" RecorderReader to filter out <K,V> pairs which don't pass the filter
 * conditions.
 * */
public class FilterRecordReader<K, V> extends RecordReader<K, V> {

  private RecordReader<K, V> realRecordReader;

  private BinaryExpression filter;
  HashMap<String, Method> columnName2Method;
  private static final Logger LOG = Logger.getLogger(FilterRecordReader.class);
  long totalQualifiedRows = 0;
  long totalScannedRows = 0;


  public FilterRecordReader(BinaryExpression filter, HashMap<String, Method> columnName2Method) {
    this.filter = filter;
    this.columnName2Method = columnName2Method;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    realRecordReader= new BlockIndexedFileInputFormat<K, V>().getRealRecordReader(split, context);
    realRecordReader.initialize(split, context);
  }

  /**
   * repeatedly call the real RecordReader's nextKeyValue() method until a
   * qualified row/<K,V> pair is found
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    // filter current key/value based on searching conditions;
    while (realRecordReader.nextKeyValue()) {
      totalScannedRows++;
      if (passConditions(realRecordReader.getCurrentValue())){
        totalQualifiedRows++;
        return true;
      }
    }
    return false;
  }

  @SuppressWarnings("rawtypes")
  private boolean passConditions(V value) throws IOException {
    try{
      BinaryWritable v =(BinaryWritable) value;
      Object o= v.get();
      return evalFilter(o,filter);
    } catch (Exception e) {
      LOG.error("cannot read the value in FilterRecordReader",e);
      throw new IOException("cannot read the value class",e);
    }
  }

  private boolean evalFilter(Object o, BinaryExpression filter) throws IOException {
    if ( filter.getOpType() ==  Expression.OpType.OP_EQ) {   //"leaf node"
      Column col = (Column) (( filter.getLhs() instanceof Column) ? filter.getLhs():filter.getRhs());
      String searchedValue = (String) ((Const) (( filter.getLhs() instanceof Const) ? filter.getLhs():filter.getRhs())).getValue();
      Method method =  columnName2Method.get(col.getName());
      try {
        return searchedValue.equals(method.invoke(o));
      } catch (Exception e) {
        throw new IOException("cannot get value from reflect.method", e);
      }
    }

    BinaryExpression lhs =  (BinaryExpression) filter.getLhs();
    BinaryExpression rhs =  (BinaryExpression) filter.getRhs();

    if (filter.opType == Expression.OpType.OP_AND)
      return evalFilter(o,lhs) && evalFilter(o,rhs);
    else
      return  evalFilter(o,lhs) || evalFilter(o,rhs);
  }

  /**
   * call the real RecordReader's getCurrentKey() method
   */
  @Override
  public K getCurrentKey() throws IOException, InterruptedException {
    return realRecordReader.getCurrentKey();
  }

  /**
   * call the real RecordReader's getCurrentValue() method
   */
  @Override
  public V getCurrentValue() throws IOException, InterruptedException {
    return realRecordReader.getCurrentValue();
  }

  /**
   * call the real RecordReader's getProgress() method
   */
  @Override
  public float getProgress() throws IOException, InterruptedException {
    return realRecordReader.getProgress();
  }

  /**
   * call the real RecordReader's close() method
   */
  @Override
  public void close() throws IOException {
    realRecordReader.close();
    LOG.debug("qualified #rows:" + totalQualifiedRows);
    LOG.debug("scanned #rows:" + totalScannedRows);
  }
}