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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.pig.LoadFunc;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;

import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.pig.load.FilterLoadFunc;
import com.twitter.elephantbird.util.TypeRef;
import com.twitter.elephanttwin.gen.FileIndexDescriptor;
import com.twitter.elephanttwin.gen.IndexedField;
import com.twitter.elephanttwin.util.HdfsUtils;

/**
 * This PigLoader "wraps" over a real PigLoader. It adds the
 * capability of reporting to Pig what filtering conditions can be pushed down
 * because of the existence of indexed files for the input files/paths to work
 * on. Example to use this new Loader:
 * <pre>
 * {@code
  T1 = LOAD '/user/mep/testdata' USING
              com.twitter.elephanttwin.retrieval.IndexedPigLoader(
                  'com.twitter.elephantbird.pig.load.LzoThriftB64LinePigLoader',
                  'com.mycompany.thrift.gen.LogEvent',
                  '/user/test/index/hdfs/index001');
  }
 * </pre>
 * If the last index directory parameter optional and if it is omitted, it'll use the value from -Dindex.hdfs.directory
 * or from the configuration file.
 */
public class IndexedPigLoader extends FilterLoadFunc {
  protected TypeRef<?> typeRef = null;
  protected String realPigLoaderName;
  protected String realInpuFormatName;
  protected String valueClassName;
  protected String indexDir;
  protected String contextSignature;

  public static final String INDEXDIRECTORY = "index.hdfs.directory";
  private static final Logger LOG = Logger.getLogger(IndexedPigLoader.class);

  public IndexedPigLoader(String realPigLoaderName, String valueClassName) {
    this(realPigLoaderName, valueClassName, "");
  }

  public IndexedPigLoader(String realPigLoaderName, String valueClassName,
      String indexDir) {
    super(null);
    this.realPigLoaderName = realPigLoaderName;
    this.valueClassName = valueClassName;
    this.indexDir = indexDir;
    loader = (LoadFunc) PigContext.instantiateFuncFromSpec(String.format(
        "%s('%s')", realPigLoaderName, valueClassName));
    try {
      this.realInpuFormatName = loader.getInputFormat().getClass().getName();
    } catch (IOException e) {
      LOG.error(e);
      throw new RuntimeException(e);
    }
  }

  /**
   * set up LZOHDFSBlockIndexedInputFormat options to be used by the real
   * PigLoader/InputFormat
   */
  @Override
  public void setLocation(String location, Job job) throws IOException {
    loader.setLocation(location, job);
    Properties props = getUDFProperties();
    // Filter conditions were set up through the setPartitionFilter
    // and getPartitionFilter mechanism,

    String filterConditions = props.getProperty(BlockIndexedFileInputFormat.FILTERCONDITIONS);
    if (indexDir == null || indexDir.equals(""))
      indexDir = job.getConfiguration().get(IndexedPigLoader.INDEXDIRECTORY);
    BlockIndexedFileInputFormat.setSearchOptions(job, realInpuFormatName, valueClassName, indexDir, filterConditions);
  }

  /**
   * return the "wrap" InputFormat class LZOHDFSBlockIndexedInputFormat for all
   * LZO based PigLoader.
   */
  @SuppressWarnings("rawtypes")
  @Override
  public InputFormat getInputFormat() throws IOException {
    BlockIndexedFileInputFormat format = new BlockIndexedFileInputFormat();
    // deserialize using PigContext
    Properties props = getUDFProperties();
    Expression.BinaryExpression filter;
    String filterConditions = props.getProperty(BlockIndexedFileInputFormat.FILTERCONDITIONS);
    filter = Expression.getFilterCondition(filterConditions);
    format.setSearchFilter(filter);
    return format;
  }
  /**
   * report to pig what columns have been indexed before. The current
   * implementation only reports the columns indexed on all input files the
   * PigLoader need to work on.
   */
  @Override
  public String[] getPartitionKeys(String location, Job job) throws IOException {
    return getUnionedPartitionKeys(location, job);
  }

  private String[] getUnionedPartitionKeys(String location, Job job)
      throws IOException {
    /**
     * report what columns have been indexed before. The current implementation
     * only reports the columns indexed on all input files the PigLoader need to
     * work on. This is done by inspecting the FileIndexDesriptor of each input
     * file
     */

    if (location == null || location.equals(""))
      return null;

    Configuration conf = job.getConfiguration();
    FileSystem fs = FileSystem.get(conf);

    if (!fs.exists(new Path(indexDir))) {
      LOG.info("index dir:" + indexDir
          + " does not exist, no indexes will be used");
      return null;
    }
    LOG.info("checking directory:" +new Path(indexDir + new Path(location).toUri().getPath()));
    FileStatus[] fileStatues = fs.globStatus(new Path(indexDir + new Path(location).toUri().getPath()));

    if (fileStatues == null || fileStatues.length == 0 ) {
      LOG.info("index dir:" + indexDir + location
          + " does not have indexes, no indexes will be used");
      return null;
    }

    // return all indexed column names from all base file under location which have been previously indexed.
    HashSet<String> indexedColumns = new HashSet<String>();
    List<FileStatus> indexMetaFiles = new ArrayList<FileStatus>();
    for(FileStatus status: fileStatues){
      HdfsUtils.addInputPathRecursively(indexMetaFiles, fs, status.getPath(),
          HdfsUtils.hiddenDirectoryFilter, indexMetaPathFilter);
    }
    LOG.info("found " + indexMetaFiles.size() + " index descriptor files");

    for (FileStatus indexMetafile : indexMetaFiles) {
      FSDataInputStream in = fs.open(indexMetafile.getPath());
      ThriftWritable<FileIndexDescriptor> writable = ThriftWritable
          .newInstance(FileIndexDescriptor.class);
      writable.readFields(in);
      FileIndexDescriptor indexDescriptor = writable.get();

      List<IndexedField> indexedFields = indexDescriptor.getIndexedFields();
      in.close();
      for (IndexedField field : indexedFields) {
        String colName = field.getFieldName();
        indexedColumns.add(colName);
      }
    }

    if (indexedColumns.size() == 0){
      return null;
    }

    return indexedColumns.toArray(new String[indexedColumns.size()]);
  }

  private final PathFilter indexMetaPathFilter = new PathFilter() {
    // avoid hidden files and directories.
    @Override
    public boolean accept(Path path) {
      String name = path.getName();
      return name.equals(BlockIndexedFileInputFormat.INDEXMETAFILENAME);
    }
  };

  /**
   * get the filter conditions from Pig and set up
   */
  @Override
  public void setPartitionFilter(org.apache.pig.Expression partitionFilter) throws IOException {
    // supporting one index only for now
    if (!Expression.isSupported(partitionFilter))
      throw new IOException("not supported PIG filter condition " + partitionFilter);
    Expression.BinaryExpression filter = Expression.newInstance(partitionFilter);
    String filterCondString = ObjectSerializer.serialize(filter);
    Properties props = getUDFProperties();
    props.setProperty(BlockIndexedFileInputFormat.FILTERCONDITIONS, filterCondString);
  }


  /** UDF properties for this class based on context signature */
  protected Properties getUDFProperties() {
    return UDFContext.getUDFContext().getUDFProperties(this.getClass(),
        new String[] { contextSignature });
  }

  @Override
  public void setUDFContextSignature(String signature) {
    this.contextSignature = signature;
  }
}