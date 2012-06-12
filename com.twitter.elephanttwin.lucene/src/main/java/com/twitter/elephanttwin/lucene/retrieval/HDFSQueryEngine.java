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
package com.twitter.elephanttwin.lucene.retrieval;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.twitter.common.base.MorePreconditions;
import com.twitter.elephanttwin.lucene.HDFSDirectory;

/**
 * HDFSQueryEngine provides methods for performing queries against a Lucene index stored in hdfs.
 */
public class HDFSQueryEngine {
  private static final int MAX_HITS = 1000000;

  private final QueryParser parser;
  private final IndexSearcher searcher;
  private static final Random rand = new Random();

  public HDFSQueryEngine(Path path, FileSystem fs, String defaultField, Analyzer analyzer) throws IOException {
    Preconditions.checkNotNull(path);
    Preconditions.checkNotNull(fs);
    Preconditions.checkNotNull(analyzer);
    MorePreconditions.checkNotBlank(defaultField);

    Directory dir = new HDFSDirectory(path, fs);
    IndexReader reader = IndexReader.open(dir);
    this.searcher = new IndexSearcher(reader);
    this.parser = new QueryParser(Version.LUCENE_40, defaultField, analyzer);
  }

  public int count(String q) {
    // TODO(jimmy): We can make this much more efficient; counting without retrieving all hits.
    TopDocs results = query(q, MAX_HITS);
    return results.totalHits;
  }

  public TopDocs query(String q) {
    return query(q, MAX_HITS);
  }

  public TopDocs query(String q, int numHits) {
    MorePreconditions.checkNotBlank(q);
    Query query;
    try {
      query = parser.parse(q);
    } catch (ParseException e) {
      throw new IllegalArgumentException("Invalid query: " + q, e);
    }
    try {
      return searcher.search(query, numHits);
    } catch (IOException e) {
      throw new RuntimeException("Error searching the index!", e);
    }
  }

  public Document doc(int n) {
    try {
      return searcher.doc(n);
    } catch (CorruptIndexException e) {
      throw new RuntimeException("Error searching the index!", e);
    } catch (IOException e) {
      throw new RuntimeException("Error searching the index!", e);
    }
  }

  /**
   * retrieve up to numSamples random docs from hits
   * @param hits documents you want a sample of
   * @param numSamples how many (max) samples to collect
   * @return numSamples samples, or all docs in hits if there are
   *         less than or equal to number of docs in hits than numSamples
   *
   * NOTE: this will mutate the order of hits.scoreDocs
   */
  public List<Document> getNSampleDocs(TopDocs hits, int numSamples) {
    if (numSamples <=0 || hits.scoreDocs.length == 0) {
      return Collections.emptyList();
    }
    // use Bill's fancy pants random sampling method
    List<ScoreDoc> samples = getNSamples(hits.scoreDocs, numSamples);
    return Lists.transform(samples, new Function<ScoreDoc, Document>() {
      @Override
      public Document apply(ScoreDoc scoreDoc) {
        return doc(scoreDoc.doc);
      }
    });
  }

  public int getNumDocs() {
    return searcher.getIndexReader().numDocs();
  }

  /**
   * retrieve up to numSamples random elements from arr
   * @param arr array you want a sample of
   * @param numSamples how many (max) samples to collect
   * @return numSamples samples, or all elements in arr if there are
   *         less than or equal to number of elements in arr than numSamples
   *
   * NOTE: this will mutate the order of arr
   */
  public static <T> List<T> getNSamples(T[] arr, int numSamples) {
    if (arr.length <= numSamples) {
      return Lists.newArrayList(arr);
    } else {
      List<T> samples = Lists.newLinkedList();
      for (int i = 0; i < numSamples; i++) {
        int lastValidItem = arr.length - samples.size() - 1;
        int randIndex = rand.nextInt(lastValidItem+1);

        T sample = arr[randIndex];
        samples.add(sample);

        arr[randIndex] = arr[lastValidItem];
        arr[lastValidItem] = sample;
      }
      return samples;
    }
  }
}
