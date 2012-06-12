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

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;
import com.twitter.elephanttwin.util.Functional;
import com.twitter.elephanttwin.util.Functional.F1;
import com.twitter.elephanttwin.util.ReflectionHelper;
/**
 * <p>Mapper for indexing a set of FileSplits.</p>
 *
 * <p>Mapper creates a "fake" document for each split, where the
 * contents (i.e., terms) are determined by provided conf via etwin.fields_to_index.</p>
 * <p>
 * To provide more control and avoid documents
 * that are too large (which causes Java heap issues on the Lucene indexing end), this class
 * provides a mechanism to limit the "size" of each fake document, in terms of number of records
 * processed: therefore, it is possible to have multiple "fake" documents associated with each
 * split.</p>
 */
public class HadoopSplitIndexingMapper<W extends Writable> extends
    Mapper<LongWritable, W, LongWritable, HadoopSplitDocument> {
  private static final Logger LOG = Logger.getLogger(HadoopSplitIndexingMapper.class);

  static final String FIELDS_TO_INDEX_KEY = "etwin.fields_to_index";
  static final String FIELD_VALUE_EXTRACTOR_PREFIX = "etwin.extractor_for_";

  // Maximum size of the "fake" document.
  private static final int MAXIMUM_DOCUMENT_SIZE = Integer.MAX_VALUE;

  private int cnt = 0;
  private FileSplit split;
  private Map<String, Functional.F1<String, W>> keyExtractorMap = Maps.newHashMap();
  Map<String, Set<W>> keyValueListMap = Maps.newHashMap();
  HadoopSplitDocument doc = new HadoopSplitDocument();

  @SuppressWarnings("unchecked")
  @Override
  public void setup(Context context) throws IOException {
    // Record the FileSplit.  It's offset will serve as the docid.
    split = (FileSplit) context.getInputSplit();
    String[] fieldsToIndex = context.getConfiguration().getStrings(FIELDS_TO_INDEX_KEY);
    for (String fieldToIndex : fieldsToIndex) {
      String className = context.getConfiguration().get(FIELD_VALUE_EXTRACTOR_PREFIX + fieldToIndex);
      Class<Functional.F1<String, W>> extractorClass;
      try {
        extractorClass = (Class<F1<String, W>>) ReflectionHelper.getAnyClassByName(className);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
      Functional.F1<String, W> extractor = ReflectionHelper.createClassFromName(className, extractorClass);
      keyExtractorMap.put(fieldToIndex, extractor);
    }
    doc.setSplit(split);
  }

  @Override
  public void map(LongWritable key, W writable, Context context)
  throws IOException, InterruptedException {
    cnt++;
    for (Entry<String, Functional.F1<String, W>> extractorEntry : keyExtractorMap.entrySet()) {
      String s = extractorEntry.getValue().eval(writable);
      doc.addKeyValue(extractorEntry.getKey(), s);
    }

      // If current "fake" document gets too large, flush out.
    if (cnt % MAXIMUM_DOCUMENT_SIZE == 0) {
      emitDocument(context);
      doc.clear();
      doc.setSplit(split);
    }
  }

  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    emitDocument(context);
  }

  private void emitDocument(Context context) throws IOException, InterruptedException {
    // TODO: I'm still not getting the diff between the offset and split start. Shouldn't we be increasing the offset in map?
    doc.setOffset(split.getStart());
    doc.setCnt(cnt);
    context.write(new LongWritable(split.getStart()), doc);
  }
}
