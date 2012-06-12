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

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;

import com.twitter.elephanttwin.util.ReflectionHelper;

/**
 * <p>
 * The general indexing flow is as follows: the mappers process input records, and pass onto
 * reducers, which perform the actual indexing in Lucene. The number of reducers is equal to the
 * number of shards (partitions), i.e., each reducer builds an index partition independently.
 * </p>
 *
 * <p>
 * In the reducer, the Lucene documents are added to the inverted index on local disk. After the
 * indexing process is complete, the index is then copied over to HDFS. Implementations must specify
 * how input key-value pairs to the reducer are converted into Lucene documents.
 * </p>
 *
 * @author jimmy
 */
public abstract class AbstractLuceneIndexingReducer<KIN, VIN> extends Reducer<KIN, VIN, NullWritable, NullWritable> {
  private static final Logger LOG = Logger.getLogger(AbstractLuceneIndexingReducer.class);

  // These are the names of the properties that the caller needs to set.
  public static final String HDFS_INDEX_LOCATION = "hdfsIndexLocation";
  public static final String ANALYZER = "analyzer";
  public static final String SIMILARITY = "similarity";

  private static enum HEARTBEAT { BEAT }
  private static enum RECORDS { PROCESSED, SKIPPED }

  private IndexBuilder indexer;
  private File tmpIndex;

  private int cnt = 0;
  private int skipped = 0;

  // Start the heartbeat thread to let the TT know we're still alive.
  private Thread heartbeatThread;

  @Override
  public void setup(Reducer<KIN, VIN, NullWritable, NullWritable>.Context context) throws IOException {
    LOG.info("Starting up indexer...");
    heartbeatThread = new ReducerHeartBeatThread(context);
    heartbeatThread.start();

    String tmp = context.getConfiguration().get("hadoop.tmp.dir");
    // Use the task attempt id as the index directory name to ensuring uniqueness.
    String shardName = "index-" + context.getTaskAttemptID().toString();
    tmpIndex = new File(tmp, shardName);
    LOG.info("tmp index location: " + tmpIndex);

    // Initialize the analyzer, defaulting to Lucene's SimpleAnalyzer.
    String analyzerClassName =
      context.getConfiguration().get(ANALYZER, "org.apache.lucene.analysis.SimpleAnalyzer");
    Analyzer analyzer = ReflectionHelper.<Analyzer>createClassFromName(analyzerClassName, Analyzer.class);
    if (analyzer == null) {
      throw new RuntimeException("Unable to create analyzer!");
    }
    LOG.info("analyzer created: " + analyzerClassName);

    // Initialize the analyzer, defaulting to Lucene's DefaultSimilarity
    String similarityClassName =
      context.getConfiguration().get(SIMILARITY, DefaultSimilarity.class.getName());
    LOG.info("Similarity: "+ similarityClassName);
    Similarity similarity = ReflectionHelper.createClassFromName(similarityClassName, Similarity.class);
    if (similarity == null) {
      throw new RuntimeException("Unable to create similarity!");
    }
    LOG.info("similarity created: " + similarityClassName);

    indexer = new IndexBuilder(tmpIndex, analyzer, similarity);
    indexer.initialize();
  }

  @Override
  public void reduce(KIN key, Iterable<VIN> values, Context context)
      throws IOException, InterruptedException {
    // There may be multiple values associated with the same key, so make sure we index all of them.
    for (VIN value : values) {
      // Calls implementation-specific method to build the Lucene document.
      Document doc = buildDocument(key, value);

      // Add to index, noting failures.
      if (indexer.add(doc)) {
        context.getCounter(RECORDS.PROCESSED).increment(1);
        cnt++;
      } else {
        context.getCounter(RECORDS.SKIPPED).increment(1);
        skipped++;
      }

      if (cnt % 10000 == 0) {
        LOG.info(cnt + " records indexed...");
      }
    }
  }

  /**
   * Builds the Lucene document to be indexed from input key-value pairs.
   */
  protected abstract Document buildDocument(KIN k, VIN v);

  @Override
  public void cleanup(Reducer<KIN, VIN, NullWritable, NullWritable>.Context context) throws IOException {
    // This may take a while...
    indexer.close();
    LOG.info("Done finalizing index!");

    LOG.info(cnt + " records added to the index");
    LOG.info(skipped + " records skipped");

    // Copy from local back to HDFS.
    Path destination = new Path(context.getConfiguration().get(HDFS_INDEX_LOCATION));
    LOG.info("final index destination: " + destination);
    LOG.info("copying from " + tmpIndex + " to " + destination);

    FileSystem fs = FileSystem.get(context.getConfiguration());

    if (!fs.exists(destination)) {
      fs.mkdirs(destination);
    }

    fs.copyFromLocalFile(new Path(tmpIndex.getAbsolutePath()), destination);
    LOG.info("copying complete!");

    // Clean up local tmp directory.
    FileUtil.fullyDelete(tmpIndex);
    LOG.info("local directory " + tmpIndex + " removed!");

    heartbeatThread.interrupt();
  }

  // This is a heartbeat thread that lets the Hadoop TT know we're still alive, so the task attempt
  // doesn't time out (in case index optimization takes longer than the timeout interval.
  private class ReducerHeartBeatThread extends Thread {
    private final Reducer<KIN, VIN, NullWritable, NullWritable>.Context context;

    public ReducerHeartBeatThread(
        Reducer<KIN, VIN, NullWritable, NullWritable>.Context context) {
      this.context = context;
    }

    @Override
    public void run() {
      while (true) {
        // Makes sure Hadoop is not killing the task in case the optimization takes longer than the
        // task timeout. To let the TT know we're still alive, increment a counter.
        try {
          sleep(60000);
          LOG.info("Sending heartbeat to TaskTracker.");
          context.getCounter(HEARTBEAT.BEAT).increment(1);
          context.setStatus("sending heartbeat...");
        } catch (InterruptedException e) {
          break;
        }
      }
    }
  }
}
