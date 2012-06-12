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

import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.elephanttwin.util.ReflectionHelper;

/**
 * This tools allows you to perform a text search on Lucene indexes stored in Hadoop
 * straight from the command line.
 */
public class HDFSRetrievalDemo extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(HDFSRetrievalDemo.class);

  private static class Config {
    @NotNull @CmdLine(name = "index", help = "index location")
    public static final Arg<String> index = Arg.create();

    @NotNull @CmdLine(name = "query", help = "query")
    public static final Arg<String> query = Arg.create();

    @NotNull @CmdLine(name = "num_hits", help = "number of hits")
    public static final Arg<Integer> numHits = Arg.create();

    @NotNull @CmdLine(name = "default_field", help = "default field to search on")
    public static final Arg<String> defaultField = Arg.create();

    @NotNull @CmdLine(name = "return_fields",
        help = "comma separated list of fields to print for retrieved document")
    public static final Arg<List<String>> returnFields = Arg.create();

    // really, we could figure this out from the metadata stored next to the index.
    @CmdLine(name = "analyzer", help = "Lucene analyzer to use")
    public static final Arg<String> analyzer = Arg.create(WhitespaceAnalyzer.class.getName());
  }

  @Override
  public int run(String[] args) throws Exception {
    Config params = new Config();

    Analyzer analyzer = ReflectionHelper.<Analyzer>createClassFromName(Config.analyzer.get(), Analyzer.class);
    if (analyzer == null) {
      throw new RuntimeException("Unable to create analyzer!");
    }

    HDFSQueryEngine qe = new HDFSQueryEngine(new Path(Config.index.get()), FileSystem.get(getConf()),
        Config.defaultField.get(), analyzer);
    TopDocs rs = qe.query(Config.query.get(), Config.numHits.get());

    for (ScoreDoc scoreDoc : rs.scoreDocs) {
      Document hit = qe.doc(scoreDoc.doc);
      for (String field : Config.returnFields.get()) {
        System.out.print(hit.getField(field) + " ");
      }
      System.out.print("\n");
    }

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new HDFSRetrievalDemo(), args);
  }
}
