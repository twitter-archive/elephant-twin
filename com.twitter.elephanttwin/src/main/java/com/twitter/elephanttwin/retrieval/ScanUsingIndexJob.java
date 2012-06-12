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

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.elephanttwin.util.HdfsUtils;
import com.twitter.elephanttwin.verification.IndexScanMapper;

/**
 * ScanUsingIndexJob is for internal testing only.
 * It is used to verify searching using indexes on
 * every unique key from the list of input base files.
 * It takes in a list of input paths, it uses indexes to find indexed blocks
 * containing the key and generates  the total counts found through index and
 *  store the results in a single file.
 * Then the results can be compared against a grouping-counting pig script using
 * regular pigloader on the same input dirs. <p>
 *
 * ScanUsingIndexJob could take a long time to finish if there are more than 10000
 * unique keys in the input files.
 * This is because it will have to scan the files
 * as many times as the number of unique keys though using indexes. This is to
 * completely simulate the searching behavior on each key. <p>
 *
 * The number of mappers started for this testing depends on the number of
 * indexes files from previously run indexing job.
 * It is recommended that for speeding up this testing,
 * the indexing job should use a large number of
 * reducers (-num_partitions) so  ScanUsingIndexJob can use more mappers to
 * finish sooner.<p>

 * Command line example:
 <pre>
 {@code
  hadoop jar elephant-twin-1.0.jar com.twitter.elephanttwin.retrieval.ScanUsingIndexJob
   -index=/path/to/hdfs/indexes/
   -input=/logs/events/2012/04/15/00
   -inputformat=com.twitter.elephantbird.mapreduce.input.LzoThriftB64LineInputFormat
   -value_class=com.mycompany.thrift.gen.LogEvent
   -columnname=event_name
   -output=/tmp/scan_results/
 }
 </pre>
 */
public class ScanUsingIndexJob extends Configured implements Tool {

  protected static Logger LOG = Logger.getLogger(ScanUsingIndexJob.class);

  // the name for the index meta file
  public final static String INDEXMETAFILENAME = "index.indexmeta";
  public final static String CURRENT_INDEXING_JOBS = "index.jobs.number";
  public final static int DEFAULT_CURRENT_JOBS = 30;
  protected IndexConfig params;

  /**
   * Input parameters configuration for running the indexing job.
   */
  public static class IndexConfig {
    @NotNull
    @CmdLine(name = "index", help = "index location")
    private static final Arg<String> index = Arg.create();
    private String indexVal;
    public void setIndex(String index) {
      indexVal = index;
    }
    public String getIndex() {
      return indexVal == null ? index.get() : indexVal;
    }

    @NotNull
    @CmdLine(name = "input", help = "one or more paths to input data, comma separated")
    private static final Arg<List<String>> input = Arg.create();
    private List<String> inputVal;
    public void setInput(List<String> input) {
      inputVal = input;
    }
    public List<String> getInput() {
      return inputVal == null ? input.get() : inputVal;
    }

    @NotNull
    @CmdLine(name = "output", help = "output file location")
    private static final Arg<String> output = Arg.create();
    private String outputVal;
    public void setOutput(String output) {
      outputVal = output;
    }
    public String getOutput() {
      return outputVal == null ? output.get() : outputVal;
    }

    @NotNull
    @CmdLine(name = "inputformat", help = "acutal inputformat class name to be used (example com.twitter.elephantbird.mapreduce.input.LzoThriftB64LineInputFormat")
    private static final Arg<String> inputformat = Arg.create();
    private String inputFormatVal;
    public void setinputFormat(String inputFormat) {
      inputFormatVal = inputFormat;
    }
    public String getinputFormat() {
      return inputFormatVal == null ? inputformat.get() : inputFormatVal;
    }

    @NotNull
    @CmdLine(name = "value_class", help = "class name used to read values from HDFS (example com.twitter.clientapp.gen.LogEvent")
    private static final Arg<String> value_class = Arg.create();
    private String valueClassVal;
    public void setValueClass(String valueClass) {
      valueClassVal = valueClass;
    }
    public String getValueClass() {
      return valueClassVal == null ? value_class.get() : valueClassVal;
    }

    @NotNull
    @CmdLine(name = "columnname", help = "column name to be indexed")
    private static final Arg<String> columnname = Arg.create();
    private String columnNameVal;
    public void setColumnName(String columnName) {
      columnNameVal = columnName;
    }
    public String getColumnName() {
      return columnNameVal == null ? columnname.get() : columnNameVal;
    }

  }

  public final static PathFilter indexDataFilter = new PathFilter() {

    @Override
    public boolean accept(Path path) {
      String name = path.getName();
      if (name.equals("data"))
        return true;
      else
        return false;
    }
  };

  public final static PathFilter hiddenDirectoryFilter = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      String name = path.getName();

      if (!name.startsWith(".") && !name.startsWith("_"))
        return true;
      else
        return false;
    }

  };

  @Override
  public int run(String[] args) throws Exception {

    params = new IndexConfig();

    LOG.info(" - input: " + Joiner.on(" ").join(params.getInput()));
    LOG.info(" - output: " + IndexConfig.output.get());

    Configuration conf = getConf();

    Path outputDir = new Path(params.getOutput());
    FileSystem fs = outputDir.getFileSystem(conf);
    fs.delete(outputDir, true);


    int totalInputFiles = 0;
    List<FileStatus> stats = Lists.newArrayList();
    for (String s : params.getInput()) {
      Path spath = new Path(IndexConfig.index.get() + s);
      HdfsUtils.addInputPathRecursively(stats, fs, spath, hiddenDirectoryFilter,
          indexDataFilter);
    }



    totalInputFiles = stats.size();
    LOG.info(totalInputFiles + " total index files to be scanned");

    conf.set(IndexScanMapper.searchColumnName , params.getColumnName());
    Job job = new Job(new Configuration(conf));
    job.setJarByClass(getClass());
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, new Path(params.getOutput()));

    for (FileStatus file : stats)
      FileInputFormat.addInputPath(job, file.getPath());

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    job.setNumReduceTasks(1);

    job.setMapperClass(IndexScanMapper.class);
    job.setCombinerClass(LongSumReducer.class);
    job.setReducerClass(LongSumReducer.class);

    job.setJobName("ScanUsingIndexJob:" + IndexConfig.input.get());
    BlockIndexedFileInputFormat.setSearchOptions(job, params.getinputFormat(),
        params.getValueClass(), params.getIndex(), null);
    job.waitForCompletion(true);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    GenericOptionsParser optParser = new GenericOptionsParser(args);
    ToolRunner.run(optParser.getConfiguration(), new ScanUsingIndexJob(),
        optParser.getRemainingArgs());
  }
}