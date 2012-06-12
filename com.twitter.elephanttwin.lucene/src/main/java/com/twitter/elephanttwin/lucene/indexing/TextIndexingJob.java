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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.thirdparty.guava.common.base.Joiner;
import org.apache.hadoop.thirdparty.guava.common.collect.Lists;

import com.twitter.elephanttwin.gen.DocType;
import com.twitter.elephanttwin.gen.IndexedField;

public class TextIndexingJob extends AbstractLuceneIndexingJob {

  @Override
  protected void setupJob(Job job) {
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setMapperClass(TextIndexingMapper.class);
    job.setReducerClass(TextIndexingReducer.class);
  }

  @Override
  protected String getJobName(IndexConfig params) {
    return "ETwin Indexing of " + Joiner.on(",").join(params.input.get());
  }

  @Override
  protected int getIndexVersion() {
    return 0;
  }

  @Override
  protected List<IndexedField> getIndexedFields() {
    IndexedField field = new IndexedField();
    field.setFieldName("query");
    field.setAnalyzed(true);
    field.setIndexed(true);
    field.setStored(true);
    return Lists.newArrayList(field);
  }

  @Override
  protected DocType getExpectedDocType() {
    return DocType.RECORD;
  }

}
