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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.pig.impl.util.ObjectSerializer;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.elephantbird.mapreduce.io.BinaryWritable;
import com.twitter.elephanttwin.retrieval.Expression.BinaryExpression;

/**
 * TestIndexedReader is for internal testing and debugging only.
 * It takes a start-offset and end-offset, construct the corresponding inputsplit.
 * It also takes a column name and a search name to construct a filter to filter
 * out rows.<p>
 *
 * Command line example:
 <pre>
 {@code
  hadoop jar my.jar com.twitter.elephanttwin.retrieval.TestIndexedReader
    -input=/logs/events/2012/04/15/00/some_log.lzo
   -inputformat=com.twitter.elephantbird.mapreduce.input.LzoThriftB64LineInputFormat
   -value_class=com.mycompany.thrift.gen.LogEvent
   -columnname=event_name
   -searchvalue=value_to_search_for
   -startoffset=1189692769
   -endoffset=1189726334
   -output=/user/me/testresults/
 }
 </pre>
 */
public class TestIndexedReader extends Configured implements Tool {

  protected static Logger LOG = Logger.getLogger(TestIndexedReader.class);

  // the name for the index meta file
  public final static String INDEXMETAFILENAME = "index.indexmeta";
  public final static String CURRENT_INDEXING_JOBS = "index.jobs.number";
  public final static int DEFAULT_CURRENT_JOBS = 30;
  protected CmdParams params;

  /**
   * Input parameters configuration for running the indexing job.
   */
  public static class CmdParams {
    @NotNull @CmdLine(name = "input", help = "the input file")
    public static final Arg<String> input = Arg.create();

    @NotNull @CmdLine(name = "output", help = "output file location")
    public static final Arg<String> output = Arg.create();

    @NotNull @CmdLine(name = "inputformat", help = "acutal inputformat class name to be used (example com.twitter.elephantbird.mapreduce.input.LzoThriftB64LineInputFormat")
    public  static final Arg<String> inputformat = Arg.create();

    @NotNull @CmdLine(name = "value_class", help = "class name used to read values from HDFS (example com.twitter.clientapp.gen.LogEvent")
    public  static final Arg<String> value_class = Arg.create();

    @NotNull @CmdLine(name = "columnname", help = "column name to be indexed")
    public  static final Arg<String> columnname = Arg.create();

    @NotNull @CmdLine(name = "searchvalue", help = "column name to be indexed")
    public  static final Arg<String> searchvalue = Arg.create();


    @NotNull @CmdLine(name = "startoffset", help = "column name to be indexed")
    public static final Arg<Long> startoffset = Arg.create();

    @NotNull @CmdLine(name = "endoffset", help = "column name to be indexed")
    public static final Arg<Long> endoffset = Arg.create();
  }

  public class PrintBinaryMapper<M>  extends
  Mapper<LongWritable, BinaryWritable<M>, LongWritable, Text> {
    @Override
    public void map(LongWritable key, BinaryWritable<M> value, Context context)
        throws IOException, InterruptedException {
      context.write(key, new Text(value.get().toString()));
    }
  }


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

    LOG.info(" - input: " +  CmdParams.input.get());
    LOG.info(" - output: " + CmdParams.output.get());

    Configuration conf = getConf();
    BinaryExpression filter = new BinaryExpression(
        new Expression.Column(CmdParams.columnname.get()),
        new Expression.Const(CmdParams.searchvalue.get()),
        Expression.OpType.OP_EQ);

    String filterCondString = ObjectSerializer.serialize(filter);
    conf.set(OneSplitInputFormat.FILTERCONDITIONS, filterCondString);

    Path outputDir = new Path(CmdParams.output.get());
    FileSystem fs = outputDir.getFileSystem(conf);
    fs.delete(outputDir, true);

    Job job = new Job(new Configuration(conf));
    job.setJarByClass(getClass());
    job.setInputFormatClass(OneSplitInputFormat.class);
    OneSplitInputFormat.setSplit(job, CmdParams.startoffset.get(),CmdParams.endoffset.get());
    OneSplitInputFormat.setIndexOptions(job, CmdParams.inputformat.get(),
        CmdParams.value_class.get(), "/tmp", CmdParams.columnname.get());

    job.setMapperClass(PrintBinaryMapper.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, new Path(CmdParams.output.get()));
    FileInputFormat.setInputPaths(job, CmdParams.input.get());
    job.setJobName("TestIndexedLZOReader:" + CmdParams.input.get());

    job.waitForCompletion(true);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    GenericOptionsParser optParser = new GenericOptionsParser(args);
    ToolRunner.run(optParser.getConfiguration(), new TestIndexedReader(),
        optParser.getRemainingArgs());
  }
}