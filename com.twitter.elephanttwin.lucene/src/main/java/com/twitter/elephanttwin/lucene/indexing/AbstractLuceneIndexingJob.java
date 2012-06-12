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
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.search.similarities.DefaultSimilarity;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.util.Strings;
import com.twitter.elephanttwin.gen.DocType;
import com.twitter.elephanttwin.gen.FileIndexDescriptor;
import com.twitter.elephanttwin.gen.IndexType;
import com.twitter.elephanttwin.gen.IndexedField;
import com.twitter.elephanttwin.gen.ETwinIndexDescriptor;
import com.twitter.elephanttwin.indexing.AbstractSamplingIndexingMapper;
import com.twitter.elephanttwin.util.Functional.F2;
import com.twitter.elephanttwin.util.HdfsFsWalker;
import com.twitter.elephanttwin.util.HdfsUtils;
import com.twitter.elephanttwin.util.ReflectionHelper;

public abstract class AbstractLuceneIndexingJob extends Configured implements Tool {

  protected Logger LOG;

  /**
   * Extend this to add custom options.
   * TODO (dmitriy): generate the getters/setters that default to statically injected values,
   * but can be overridden.
   *
   */
  public static class IndexConfig {
    @NotNull @CmdLine(name = "input", help = "one or more paths to input data, comma separated")
    public static final Arg<List<String>> input = Arg.create();

    @NotNull @CmdLine(name = "index", help = "index location")
    public static final Arg<String> index = Arg.create();

    @NotNull @CmdLine(name = "num_partitions", help = "number of partitions")
    public static final Arg<Integer> numPartitions = Arg.create();

    @CmdLine(name = "sample_percentage", help = "index randomly-sampled percentage [1,100] of records; default=100 (index everything)")
    public static final Arg<Integer> samplePercentage = Arg.create(100);

    @CmdLine(name = "analyzer", help = "Lucene analyzer to use. WhitespaceAnalyzer is default")
    public static final Arg<String> analyzer = Arg.create(WhitespaceAnalyzer.class.getName());

    @CmdLine(name = "similarity", help = "Lucene similarity to use")
    public static final Arg<String> similarity = Arg.create(DefaultSimilarity.class.getName());

    @CmdLine(name = "wait_for_completion", help = "wait for job completion")
    public static final Arg<Boolean> waitForCompletion = Arg.create(true);
  }

  protected IndexConfig params;

  @Override
  public int run(String[] args) throws Exception {
    LOG = Logger.getLogger(this.getClass());
    params = newIndexConfig();

    LOG.info("Starting up indexer...");
    LOG.info(" - input: " + Joiner.on(" ").join(IndexConfig.input.get()));
    LOG.info(" - index: " + IndexConfig.index);
    LOG.info(" - number of shards: " + IndexConfig.numPartitions.get());

    Configuration conf = getConf();

    conf.set(AbstractLuceneIndexingReducer.HDFS_INDEX_LOCATION, IndexConfig.index.get());
    conf.set(AbstractLuceneIndexingReducer.ANALYZER, IndexConfig.analyzer.get());
    conf.set(AbstractLuceneIndexingReducer.SIMILARITY, IndexConfig.similarity.get());
    conf.setInt(AbstractSamplingIndexingMapper.SAMPLE_PERCENTAGE, IndexConfig.samplePercentage.get());

    conf.setBoolean("mapred.map.tasks.speculative.execution", false);
    conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

    Job job = new Job(conf, getJobName(params));

    // Job's constructor copies conf, we need a reference to the one job
    // is actually using
    conf = job.getConfiguration();

    job.setJarByClass(this.getClass());

    job.setNumReduceTasks(IndexConfig.numPartitions.get());

    for (String s : IndexConfig.input.get()) {
      Path spath = new Path(s);
      FileSystem fs = spath.getFileSystem(getConf());
      List<FileStatus> stats = Lists.newArrayList();
      addInputPathRecursively(stats, fs, spath, HdfsUtils.HIDDEN_FILE_FILTER);
      for (FileStatus foundStat : stats) {
        FileInputFormat.addInputPath(job, foundStat.getPath());
      }
    }

    FileOutputFormat.setOutputPath(job, new Path(IndexConfig.index.get()));

    setupJob(job);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(IndexConfig.index.get());
    FileSystem.get(conf).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    LOG.info("Job " + getJobName(params) + " started.");
    // TODO Jimmy has a parameter that controls whether we wait in Thud but not in ES.
    // when would we not want to wait?
    job.waitForCompletion(true);
    LOG.info("Job " + getJobName(params) + " Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    if (job.isSuccessful()) {
      writeIndexDescriptors(getIndexDescriptor());
    }
    return job.isSuccessful() ? 0 : 1;
  }

  protected void addInputPathRecursively(List<FileStatus> result, FileSystem fs,
      Path path, PathFilter inputFilter) throws IOException {
    for(FileStatus stat: fs.listStatus(path, inputFilter)) {
      if (stat.isDir()) {
        addInputPathRecursively(result, fs, stat.getPath(), inputFilter);
      } else {
        result.add(stat);
      }
    }
  }

  private void writeIndexDescriptors(ETwinIndexDescriptor ETwinIndexDescriptor) throws IOException {
    Configuration conf = getConf();

    FileSystem fs = (new Path(IndexConfig.index.get()).getFileSystem(conf));

    FileStatus[] fileStats = fs.globStatus(new Path(IndexConfig.index.get(), "*"));

    // We write one indexDescriptor per generated index segment.
    // Something to consider: right now it's a straight-up serialized Thrift object.
    // Would it be better to do the LzoBase64Line thing, so that we can apply our tools?
    // or extend the tools?
    for (int i = 0; i < fileStats.length; i++) {
      ETwinIndexDescriptor.setIndexPart(i);
      FileStatus stat = fileStats[i];
      Path idxPath = new Path(stat.getPath().getParent(), "_" + stat.getPath().getName() + ".indexmeta");
      FSDataOutputStream os = fs.create(idxPath, true);
      @SuppressWarnings("unchecked")
      ThriftWritable<ETwinIndexDescriptor> writable =
        (ThriftWritable<ETwinIndexDescriptor>) ThriftWritable.newInstance(ETwinIndexDescriptor.getClass());
      writable.set(ETwinIndexDescriptor);
      writable.write(os);
      os.close();
    }

  }

  /**
   * Override this if you have custom options.
   * @return a Config object that defines the options this Indexing Job understands.
   */
  protected IndexConfig newIndexConfig() {
    return new IndexConfig();
  }

  /**
     Individual indexers will want to set up:
    <li>job.setInputFormatClass(TextInputFormat.class);
    <li>job.setMapOutputKeyClass(UidTweepcredPair.class);
    <li>job.setMapOutputValueClass(Text.class);
    <li>job.setOutputKeyClass(UidTweepcredPair.class);
    <li>job.setOutputValueClass(Text.class);
    <li>job.setOutputFormatClass(NullOutputFormat.class);

    <li>job.setMapperClass(ExpertSearchIndexingMapper.class);
    <li>job.setReducerClass(ExpertSearchIndexingReducer.class);
    <li>job.setPartitionerClass(MyPartitioner.class);

   <p> Also add any extra params to the job's conf</p>
   * @param params
   * @param job
   * @param conf
   */
  protected abstract void setupJob(Job job);

  /**
   * @return Desired job name (as seen in JobTracker).
   */
  protected abstract String getJobName(IndexConfig params);

  /**
   * Populates FileIndexDescriptor with common things like name, checksum, etc.
   * @param path
   * @return
   * @throws IOException
   */
  protected FileIndexDescriptor buildFileIndexDescriptor(Path path) throws IOException {
    FileIndexDescriptor fid = new FileIndexDescriptor();
    fid.setSourcePath(path.toString());
    fid.setDocType(getExpectedDocType());
    FileChecksum cksum = path.getFileSystem(getConf()).getFileChecksum(path);
    com.twitter.elephanttwin.gen.FileChecksum fidCksum =
      new com.twitter.elephanttwin.gen.FileChecksum(cksum.getAlgorithmName(),
          ByteBuffer.wrap(cksum.getBytes()), cksum.getLength());
    fid.setChecksum(fidCksum);
    fid.setIndexedFields(getIndexedFields());
    fid.setIndexType(getIndexType());
    fid.setIndexVersion(getIndexVersion());
    return fid;
  }


  /**
   * Don't forget to bump up the IndexVersion when changing indexing algos or fields!
   * @return
   */
  protected abstract int getIndexVersion();

  /**
   * <p>
   * Tell the Indexer what fields you are indexing and what you are doing with them.
   * </p>
   * <p>
   * It's important to keep this in sync with what you are actually doing so that
   * clients that examine index metadata know what to expect and can choose an appropriate
   * index.
   * </p>
   */
  protected abstract List<IndexedField> getIndexedFields();

  /**
   * You might be indexing a "document" of individual records, or some collection of records
   * that make up a "block" (say, an LZO block or an HDFS block). Tell us which it is.
   */
  protected abstract DocType getExpectedDocType();

  /**
   * One imagines a brave new world in which we have a choice of underlying index
   * implementations (lucene on hdfs? some hbase thing? mapfiles?).
   */
  protected IndexType getIndexType() {
    return IndexType.LUCENE;
  }

  /**
   * Implementations that have custom fields to add to the IndexDescriptor
   * should override this, call super.getIndexDescriptor(), and then
   * populate options field appropriately for each index file.
   * @return ETwinIndexDescriptor
   * @throws IOException
   */
  public ETwinIndexDescriptor getIndexDescriptor() throws IOException {
    final ETwinIndexDescriptor desc = new ETwinIndexDescriptor();

    HdfsFsWalker walker = new HdfsFsWalker(getConf());
    // The silly-looking Exception wrapping is due to F3 not letting us throw
    // checked exceptions.
    try {
    walker.walk(new Path(IndexConfig.index.get()), new F2<Boolean, FileStatus, FileSystem>() {

      @Override
      public Boolean eval(FileStatus status, FileSystem fs) {
        if (!status.isDir()) {
          try {
            desc.addToFileIndexDescriptors(buildFileIndexDescriptor(status.getPath()));
          } catch (IOException e) {
            LOG.error("unable to buld a FileIndexDescriptor for " + status.getPath(),e);
            throw new RuntimeException("unable to buld a FileIndexDescriptor for " + status.getPath(), e);
          }
        }
        return true;
      }});
    } catch (RuntimeException e) {
      throw new IOException(e);
    }

    desc.options = Maps.newHashMap();
    try {
      desc.options.put(
          Strings.camelize(ReflectionHelper.nameForOption(params, "samplePercentage")),
          String.valueOf(IndexConfig.samplePercentage.get()));
      desc.options.put(
          Strings.camelize(ReflectionHelper.nameForOption(params, "analyzer")),
          IndexConfig.analyzer.get());
      desc.options.put(
          Strings.camelize(ReflectionHelper.nameForOption(params, "similarity")),
          IndexConfig.similarity.get());
    } catch (SecurityException e) {
      throw new IOException(e);
    } catch (NoSuchFieldException e) {
      throw new IOException(e);
    }
    // The idea is that as index parts are written out, they will write in their part #s.
    desc.setIndexPart(0);
    return desc;
  }

}
