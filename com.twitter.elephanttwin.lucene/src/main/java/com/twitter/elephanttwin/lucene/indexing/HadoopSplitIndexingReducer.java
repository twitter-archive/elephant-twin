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
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.twitter.elephanttwin.lucene.LuceneUtil;

/**
 * Reducer for indexing into Hadoop Splits, converts each {@link HadoopSplitDocument} into a
 * Lucene Document.
 *
 */
public class HadoopSplitIndexingReducer
    extends AbstractLuceneIndexingReducer<LongWritable, HadoopSplitDocument> {
  private static final Logger LOG = Logger.getLogger(HadoopSplitIndexingReducer.class);

  @Override
  protected Document buildDocument(LongWritable k, HadoopSplitDocument v) {
    LOG.info("Indexing document:");
    LOG.info(" offset: " + v.getOffset());
    LOG.info(" cnt: " + v.getCnt());

    // Build the document. We simply concatenate all observed
    // values for key using whitespace, and use
    // WhitespaceAnalyzer to tokenize.
    // TODO: This is suboptimal and we can add more fancyness later.
    Document doc = new Document();
    doc.add(new Field("offset", "" + v.getOffset(),
        LuceneUtil.getFieldType(LuceneUtil.STORED_NO_INDEX_NO_NORM_NO_TOKEN)));
    // Use cnt as an additional field to disambiguate in case there are multiple "fake" documents
    // with the same offset.
    doc.add(new Field("cnt", "" + v.getCnt(),
        LuceneUtil.getFieldType(LuceneUtil.STORED_NO_INDEX_NO_NORM_NO_TOKEN)));
    for (Map.Entry<String, Set<String>> entry : v.getKeyValueListMap().entrySet()) {
      List<String> strings = Lists.newArrayListWithExpectedSize(entry.getValue().size());
      for (String s : entry.getValue()) {
        strings.add(s);
      }
      doc.add(new Field(entry.getKey(), Joiner.on(" ").join(strings),
          LuceneUtil.getFieldType(LuceneUtil.NO_STORE_INDEX_NO_NORM_TOKEN)));
    }
    return doc;
  }
}
