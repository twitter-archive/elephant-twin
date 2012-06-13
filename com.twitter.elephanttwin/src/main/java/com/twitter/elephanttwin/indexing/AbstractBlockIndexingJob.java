/**
 * Copyright 2012 Twitter, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.twitter.elephanttwin.indexing;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephanttwin.gen.DocType;
import com.twitter.elephanttwin.gen.FileIndexDescriptor;
import com.twitter.elephanttwin.gen.IndexType;
import com.twitter.elephanttwin.gen.IndexedField;
import com.twitter.elephanttwin.io.ListLongPair;
import com.twitter.elephanttwin.io.LongPairWritable;
import com.twitter.elephanttwin.io.TextLongPairWritable;
import com.twitter.elephanttwin.retrieval.BlockIndexedFileInputFormat;
import com.twitter.elephanttwin.util.HdfsUtils;
import com.twitter.elephanttwin.util.YamlConfig;

/**
 *
 * @author Dmitriy V Ryaboy
 * @author Yu Xu
 *
 */
public abstract class AbstractBlockIndexingJob
  extends Configured implements Tool {

  protected Logger LOG =Logger
      .getLogger(AbstractBlockIndexingJob.class);
  public static final String YAML_INPUT_DIR = "input_file_base_directory";

  protected abstract List<String> getInput();
  protected abstract String getIndex();
  protected abstract String getInputFormat();
  protected abstract String getValueClass();
  protected abstract String getKeyValue();
  protected abstract String getColumnName();
  protected abstract Integer getNumPartitions();
  protected abstract Integer getSleepTime();
  protected abstract Boolean doOverwrite();
  protected abstract Boolean isDryRun();
  protected abstract Integer getJobPoolSize();

  /**
   * Set the MapperClass to your implementation of BlockIndexingMapper.
   * You can add implementation-specific properties to the jobconf in this method.
   * @param job
   * @return re-configured job.
   */
  protected abstract Job setMapper(Job job);

  /**
   * @return a string which will serve as the job name in Hadoop.
   */
  protected abstract String getJobName();


  /**
   * FileFilter for files we don't even want to consider
   * indexing.
   */
  protected abstract PathFilter getFileFilter();

  int totalInputFiles = 0;
  int totalFiles2Index = 0;
  Boolean noBatchJob;
  YamlConfig config;
  AtomicInteger finishedJobs = new AtomicInteger(0);
  AtomicInteger indexedFiles = new AtomicInteger(0);
  List<String> failedFiles = Collections
      .synchronizedList(new ArrayList<String>());

  /**
   * Create a FileIndexDescriptor to describe what columns have been indexed
   * @param path
   *          the path to the directory where index files are stored for the
   *          input file
   * @return FileIndexDescriptor
   * @throws IOException
   */

  protected void createIndexDescriptors(FileStatus inputFile, FileSystem fs)
      throws IOException {
    Path indexFilePath = new Path(getIndex()
        + inputFile.getPath().toUri().getRawPath());

    FileIndexDescriptor fid = new FileIndexDescriptor();
    fid.setSourcePath(inputFile.getPath().toString());
    fid.setDocType(getExpectedDocType());
    LOG.info("getting checksum from:" + inputFile.getPath());
    FileChecksum cksum = fs.getFileChecksum(inputFile.getPath());
    com.twitter.elephanttwin.gen.FileChecksum fidCksum = null;
    if (cksum != null)
      fidCksum = new com.twitter.elephanttwin.gen.FileChecksum(
          cksum.getAlgorithmName(), ByteBuffer.wrap(cksum.getBytes()),
          cksum.getLength());
    fid.setChecksum(fidCksum);
    fid.setIndexedFields(getIndexedFields());
    fid.setIndexType(getIndexType());
    fid.setIndexVersion(getIndexVersion());

    Path idxPath = new Path(indexFilePath + "/"
        + BlockIndexedFileInputFormat.INDEXMETAFILENAME);
    FSDataOutputStream os = fs.create(idxPath, true);
    @SuppressWarnings("unchecked")
    ThriftWritable<FileIndexDescriptor> writable =
    (ThriftWritable<FileIndexDescriptor>) ThriftWritable
    .newInstance(fid.getClass());
    writable.set(fid);
    writable.write(os);
    os.close();
  }

;

  protected int getIndexVersion() {
    return 1;
  }

  protected List<IndexedField> getIndexedFields() {
    List<IndexedField> indexed_fields = new ArrayList<IndexedField>();
    indexed_fields
    .add(new IndexedField(getColumnName(), false, false, false));
    return indexed_fields;
  }

  protected DocType getExpectedDocType() {
    return DocType.BLOCK;
  }

  protected IndexType getIndexType() {
    return IndexType.MAPFILE;
  }


  private class IndexingWorker implements Runnable {
    private FileStatus stat;
    private Job job;
    private FileSystem fs;

    IndexingWorker(Job job, FileStatus file, FileSystem fs) {
      stat = file;
      this.job = job;
      this.fs = fs;
    }

    @Override
    public void run() {
      try {
        Path outputDir = new Path(getIndex()
            + stat.getPath().toUri().getRawPath() + "/" + getColumnName());
        fs.delete(outputDir, true);
        long startTime = System.currentTimeMillis();

        MapFileOutputFormat.setOutputPath(job, outputDir);

        LOG.info("Job " + job.getJobName() + " started.");
        job.waitForCompletion(true);

        if (job.isSuccessful()) {
          // create an index meta file to record what columns have
          // been indexed.
          createIndexDescriptors(stat, fs);
          indexedFiles.incrementAndGet();
          LOG.info(job.getJobName() + ": indexing " + stat.getPath()
              + " succeeded and finished in "
              + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
          LOG.info("Indexed " + indexedFiles.get() + " files");

        } else {
          synchronized (failedFiles) {
            failedFiles.add(stat.getPath().toString());
          }
          LOG.info(job.getJobName() + ": indexing " + stat.getPath()
              + " failed,  " + (System.currentTimeMillis() - startTime)
              / 1000.0 + " seconds");
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        finishedJobs.incrementAndGet();
      }
    }
  }

  public int work(Calendar start, Calendar end, int batchId) {

    LOG.info("Starting up indexer...");
    LOG.info(" - input: " + Joiner.on(" ").join(getInput()));
    LOG.info(" - index: " + getIndex());
    LOG.info(" - number of reducers: " + getNumPartitions());

    Configuration conf = getConf();
    conf.setBoolean("mapred.map.tasks.speculative.execution", false);
    conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

    totalInputFiles = 0; // total number files from input directories

    try {
      ExecutorService pool = Executors.newFixedThreadPool(getJobPoolSize());

      for (String s : getInput()) {
        Path spath = new Path(s);
        FileSystem fs = spath.getFileSystem(conf);

        List<FileStatus> stats = Lists.newArrayList();

        // get all files from the input paths/directories
        HdfsUtils.addInputPathRecursively(stats, fs, spath,
            HdfsUtils.hiddenDirectoryFilter, getFileFilter());

        totalInputFiles += stats.size();
        LOG.info(" total files under " + s + ":" + stats.size());

        if (isDryRun()) {
          continue;
        }

        int filesToIndex = 0;
        for (FileStatus stat : stats) {
          // check if we still want to index the file
          if (!fileIsOk(stat, fs)) {
            continue;
          }
          if (!doOverwrite()) {
            if (hasPreviousIndex(stat, fs))
              continue;
          }
          filesToIndex++;
          totalFiles2Index++;
          Job job = setupJob(conf);
          job = setMapper(job);
          FileInputFormat.setInputPaths(job, stat.getPath());
          job.setJobName(getJobName() + ":" + stat.getPath());
          Thread.sleep(getSleepTime());
          pool.execute(new IndexingWorker(job, stat, fs));
        }

        LOG.info("total files submitted for indexing under" + s + ":"
            + filesToIndex);
      }

      if (isDryRun()) {
        return 0;
      }

      while (finishedJobs.get() < totalFiles2Index) {
        Thread.sleep(getSleepTime());
      }
      LOG.info(" total number of files from input directories: "
          + totalInputFiles);
      LOG.info(" total number of files submitted for indexing job: "
          + totalFiles2Index);
      LOG.info(" number of files successfully indexed is: " + indexedFiles);
      if (failedFiles.size() > 0)
        LOG.info(" these files were not indexed:"
            + Arrays.toString(failedFiles.toArray()));
      else
        LOG.info(" all files have been successfully indexed");
      pool.shutdown();
    } catch (Exception e) {
      LOG.error(e);
      return -1;
    }

    if (totalFiles2Index == 0)
      return 0;
    else if (totalFiles2Index != indexedFiles.get() )
      return -1;
    else
      return 1;
  }



  /**
   * Indexing jobs may want to skip some files that pass
   * the basic filter specified in getFileFilter
   * This gives them the opportunity to do so.
   *
   * TODO(dmitriy): can we just use getFileFilter()?
   *
   * @param stat file status object of the file being checked
   * @param fs fs from whence the file came
   * @return true if this file is ok to index.
   */
  protected boolean fileIsOk(FileStatus stat, FileSystem fs)
      throws IOException {
    return true;
  }

  /**
   * Sets up various job properites required for the indexing job.
   * If your implementation needs to mess with the conf, you can do so by overriding
   * this method (remember to call super.setupJob()!) or in setMapper().
   * @param conf
   * @return
   * @throws IOException
   */
  protected Job setupJob(Configuration conf) throws IOException {
    Job job = new Job(new Configuration(conf));
    job.setJarByClass(getClass());
    job.setInputFormatClass(BlockIndexedFileInputFormat.class);
    job.setReducerClass(MapFileIndexingReducer.class);
    job.setMapOutputKeyClass(TextLongPairWritable.class);
    job.setMapOutputValueClass(LongPairWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(ListLongPair.class);
    job.setPartitionerClass(TextLongPairWritable.Parititioner.class);
    job.setSortComparatorClass(TextLongPairWritable.PairComparator.class);
    job.setGroupingComparatorClass(TextLongPairWritable.KeyOnlyComparator.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);
    job.setNumReduceTasks(getNumPartitions());
    BlockIndexedFileInputFormat.setIndexOptions(job, getInputFormat(),
        getValueClass(), getIndex(), getColumnName());
    return job;
  }

  private boolean hasPreviousIndex(FileStatus stat, FileSystem fs)
      throws IOException {
    Path indexDir = new Path(getIndex() + stat.getPath().toUri().getRawPath()
        + "/" + getColumnName());
    return fs.exists(indexDir);
  }
}