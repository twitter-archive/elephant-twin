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
package com.twitter.elephanttwin.lucene.indexing;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.google.common.collect.Lists;
import com.twitter.elephanttwin.gen.DocType;
import com.twitter.elephanttwin.gen.IndexedField;

public abstract class HadoopSplitIndexingJob extends AbstractLuceneIndexingJob {

  /**
   * Override and extend this in implementations to add custom settings to the Job and Conf to
   * create lucene-based indexes that will point you at what splits contain values you are looking for.
   * You are on your own for filtering the splits appropriately before creating an MR job.. but
   * check out how this was done over MapFile-based indexes in
   * com.twitter.elephanttwin.indexing.AbstractIndexesFileInputFormat
   */
  @Override
  protected void setupJob(Job job) {
    Configuration conf = job.getConfiguration();
    conf.set("mapred.child.java.opts", "-Xmx4g");
    List<String> fieldNames = Lists.newArrayList();
    for (IndexedField field : getIndexedFields()) {
      fieldNames.add(field.getFieldName());
      conf.set(HadoopSplitIndexingMapper.FIELD_VALUE_EXTRACTOR_PREFIX +
          field.getFieldName(),
          getExtractorClassName(field.getFieldName()));
    }
    conf.setStrings(HadoopSplitIndexingMapper.FIELDS_TO_INDEX_KEY, fieldNames.toArray(new String[]{}));
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(HadoopSplitDocument.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setInputFormatClass(getInputFormatClass());

    job.setMapperClass(HadoopSplitIndexingMapper.class);
    job.setReducerClass(HadoopSplitIndexingReducer.class);
  }

  protected abstract Class<? extends InputFormat> getInputFormatClass();

  @Override
  protected String getJobName(IndexConfig params) {
    // TODO: params should be non-static.
    return this.getClass().getSimpleName() + ":index=" + IndexConfig.index.get();
  }

  public abstract String getExtractorClassName(String fieldToIndex);

  @Override
  protected DocType getExpectedDocType() {
    return DocType.BLOCK;
  }

}
