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

import com.google.common.base.Preconditions;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.similarities.DefaultSimilarityProvider;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;

/**
 * Abstract to make Lucene indexing a bit more convenient.
 *
 * @author jimmy
 */
public class IndexBuilder {
  private static final Logger LOG = Logger.getLogger(IndexBuilder.class.getName());

  private final Analyzer analyzer;
  private final Similarity similarity;
  private final File indexLocation;

  private boolean initialized = false;
  private boolean finalized = false;

  private IndexWriter writer;

  public IndexBuilder(File indexLocation, Analyzer analyzer, Similarity similarity) {
    this.indexLocation = Preconditions.checkNotNull(indexLocation);
    this.analyzer = Preconditions.checkNotNull(analyzer);
    this.similarity = Preconditions.checkNotNull(similarity);
  }

  /**
   * Initializes the object and prepares for indexing.
   */
  public void initialize() throws IOException {
    if (initialized) {
      LOG.info("UserIndexBuilder already initialized!");
      return;
    }

    LOG.info("Creating IndexWriter...");

    //TODO make sure DefaultSimilarityProvider is ok or replace it with a provider that uses similarity
    // Note: OpenMode is Create or override
    IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_40, analyzer)
        .setOpenMode(IndexWriterConfig.OpenMode.CREATE).setSimilarityProvider(new DefaultSimilarityProvider());

    writer = new IndexWriter(FSDirectory.open(indexLocation), indexWriterConfig);

    LOG.info("similarity = " + writer.getConfig().getSimilarityProvider());
    LOG.info("analyzer = " + writer.getAnalyzer());

    initialized = true;
  }

  /**
   * Adds a Lucene document to the index.
   *
   * @return true if user successfully added, false otherwise.
   */
  public boolean add(Document doc) throws IOException {
    // Make sure object has already been initialized.
    Preconditions.checkState(initialized, "UserIndexBuilder hasn't been initialized yet!");

    // Forbid adding new users after we've finalized the index.
    Preconditions.checkState(!finalized, "Can't add new users to UserIndexBuilder after finalization!");

    if (doc != null) {
      writer.addDocument(doc);
      return true;
    }

    LOG.warn("Error in document: " + doc);
    return false;
  }

  /**
   * Optimizes index and then closes it.
   */
  public void close() throws IOException {
    LOG.info("optimizing index...");

    //TODO: Find replacement for writer.optimize() in Lucene 4.0
    writer.forceMerge(1);
    LOG.info("done!");
    writer.close();

    finalized = true;
  }
}
