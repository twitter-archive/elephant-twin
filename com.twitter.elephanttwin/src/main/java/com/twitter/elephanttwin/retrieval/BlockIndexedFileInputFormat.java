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
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.log4j.Logger;
import org.apache.thrift.TBase;

import com.google.common.base.CaseFormat;
import com.google.protobuf.Message;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import com.twitter.elephantbird.util.Protobufs;
import com.twitter.elephantbird.util.TypeRef;
import com.twitter.elephanttwin.gen.FileChecksum;
import com.twitter.elephanttwin.gen.FileIndexDescriptor;
import com.twitter.elephanttwin.io.ListLongPair;
import com.twitter.elephanttwin.io.LongPairWritable;
import com.twitter.elephanttwin.retrieval.Expression.BinaryExpression;
import com.twitter.elephanttwin.retrieval.Expression.Column;
import com.twitter.elephanttwin.retrieval.Expression.Const;
import com.twitter.elephanttwin.retrieval.Expression.OpType;

/**
 * This class wraps over a real FileInputFormat based class.
 * It adds a new  functionality to use the real InputFormat class to
 * index files and search original files using indexes.
 *
 * @param <K>
 * @param <V>
 */
public class BlockIndexedFileInputFormat<K, V> extends
FileInputFormat<K, V> {
  public static String REALINPUTFORMAT = "AbstractIndexedFileInputFormat.InputFormatClassName";
  public static String VALUECLASS = "AbstractIndexedFileInputFormat.ValueClassName";
  public static String INDEXDIR = "AbstractIndexedFileInputFormat.IndexDirectory";
  public static String COLUMNNAME = "AbstractIndexedFileInputFormat.ColumnName";
  public static String FILTERCONDITIONS = "AbstractIndexedFileInputFormat.FilterConditions";
  public static String INDEXINGJOBFLAG = "AbstractIndexedFileInputFormat.isIndexingJob";
  public static String INDEXED_SPLIT_SIZE = "indexed.filesplit.maxsize";

  // the name for the index meta file
  public final static String INDEXMETAFILENAME = "index.indexmeta";

  protected FileInputFormat<K, V> inputformatClass;
  protected TypeRef<V> valueClass;

  protected String valueClassName;
  protected String indexDir = null;
  protected BinaryExpression filter;


  protected long totalBytesNewSplits;
  protected long splitLength = 0;

  private static final Logger LOG = Logger
      .getLogger(BlockIndexedFileInputFormat.class);

  public BlockIndexedFileInputFormat() {
  }


  /**
   * Go through each original inputsplit, get its file path, and check the
   *  index file,
   * a)  keep it, when there is no index prebuilt on this file
   *  (or the index file doesn't match with the base file's checksum;
   * b)  remove it when no matching value is found in existing index file;
   * c)  construct new smaller inputsplits using indexed blocks found
   * in the index file;
   */
  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {

    String inputformat = job.getConfiguration().get(REALINPUTFORMAT);
    String valueClass = job.getConfiguration().get(VALUECLASS);

    List<InputSplit> filteredList = new ArrayList<InputSplit>();

    FileInputFormat<K, V> realInputFormat = getInputFormatClass(inputformat,
        valueClass);

    List<InputSplit> splits = realInputFormat.getSplits(job);

    //if indexing jobs, don't skip any input splits.
    //if searching job but no searching filter, skip the index as well.
    if (isIndexingJob(job) || getFilterCondition(job) == null)
      return splits;

    Path prevFile = null; // remember the last input file we saw
    boolean foundIndexedFile = false; // is there a index file for
    // prevFile?
    boolean firstTime = true; // is this the first time we see this file?

    long totalOriginalBytes = 0; //the bytes to be scanned without indexes.
    totalBytesNewSplits = 0;
    long startTime = System.currentTimeMillis();
    LOG.info("start filtering out original input splits (total "+splits.size()
        + ") using indexes");
    Configuration conf = job.getConfiguration();
    long splitMaxSize;

    // for each original input split check if we can filter it out.
    for (InputSplit split : splits) {
      FileSplit fileSplit = (FileSplit) split;
      Path path = fileSplit.getPath();
      splitLength = fileSplit.getLength();
      totalOriginalBytes += fileSplit.getLength();
      splitMaxSize = Math.max(splitLength, conf.getInt(INDEXED_SPLIT_SIZE,
          conf.getInt("dfs.block.size", 256 * 1024 * 1024)));

      /*
       * for each new file we see, we first check if it has been indexed or not;
       * if not, we just add the original input split; if yes, we use the index
       * file to add filtered splits for the file
       */
      if (prevFile != null && path.equals(prevFile)) {
        firstTime = false;
      } else {
        prevFile = path;
        firstTime = true;
        foundIndexedFile = foundIndexFile(job, path);
      }

      // if no index file, we'll have to read all original input
      // splits
      if (!foundIndexedFile)
        filteredList.add(fileSplit);
      else {
        // for each file we only add once its filtered input splits using index
        // file
        if (firstTime) {
          // LOG.info("first time saw " + path
          // + ", adding filtered splits from index file");
          filteredList.addAll(getFilteredSplits(job, path,
              fileSplit.getLocations(), splitMaxSize));
        }
      }
    }

    long endTime = System.currentTimeMillis();
    LOG.info("finished filtering out input splits, now total splits:"
        + filteredList.size() + ", seconds used: " + (endTime-startTime)/1000 );
    LOG.info(String.format("total bytes to read before filtering: %s," +
        " after filtering %s, bytes ratio: %s",
        totalOriginalBytes, totalBytesNewSplits,
         totalOriginalBytes/Math.max(1,totalBytesNewSplits)));
    return filteredList;
  }

  /**
   * Use index to return a list of new IndexedFileSplits for more efficient
   * processing. Each IndexedFileSplit contains a list of small FileSplit. How
   * many FileSplits are contained in a single IndexedFileSplit is determined
   * together by the sum of all FileSplits' length in the IndexedFileSplit and a
   * configurable threshold (AbstractIndexedFileInputFormat.INDEXED_SPLIT_SIZE)
   * which by default is the default HDFS block size;
   */
  public List<InputSplit> getFilteredSplits(JobContext job, Path path,
      String[] hosts, long splitMaxSize) throws IOException {
    List<LongPairWritable> matchedBlocks = getMatchedSplits(job, path, splitMaxSize);
    return computeSplits(job, path, matchedBlocks, hosts, splitMaxSize);
  }

  public List<InputSplit> computeSplits(JobContext job, Path path,
      List<LongPairWritable> matchedBlocks, String[] hosts, long splitMaxSize)
          throws IOException {

    List<InputSplit> results = new LinkedList<InputSplit>();

    ListLongPair assignedSubBlocks = new ListLongPair();
    long totalBytes = 0;

    int len = matchedBlocks.size();

    if (len == 0)
      return results;

    for (int i = 0; i < len; i++) {
      LongPairWritable block = matchedBlocks.remove(0);
      long thisBlockSize = block.getSecond() - block.getFirst();
      totalBytesNewSplits += thisBlockSize;
      if (totalBytes + thisBlockSize <= splitMaxSize) {
        assignedSubBlocks.add(block);
        totalBytes += thisBlockSize;
      } else { // need to create a new IndexedFileSplit;
        if (assignedSubBlocks.size() > 0) {
          /*if the first split is already more than "dfs.block.size"
           * (which could happen if subblock inputformat "nudges" /align
           * subblock boundary),
          we don't want to add an empty split to the result,
          though it's not wrong to do so.
           */
          results.add(new IndexedFileSplit(path, assignedSubBlocks, hosts));
        }
        assignedSubBlocks = new ListLongPair();
        assignedSubBlocks.add(block);
        totalBytes = thisBlockSize;
      }
    }
    results.add(new IndexedFileSplit(path, assignedSubBlocks, hosts));
    return results;
  }

  /**Searching jobs call this function to set up searching job related parameters.
   * @param job
   * @param inputformatClass
   * @param valueClass
   * @param indexDir
   * @param filterConditions
   * @throws IOException
   */
  public static void setSearchOptions(Job job, String inputformatClass,
      String valueClass, String indexDir, String filterConditions)
          throws IOException {
    setOptions(job, inputformatClass, valueClass, indexDir, filterConditions,
        null, false);
  }

  /**
   * Indexing jobs call this function to set up indexing job related parameters.
   * @param job
   * @param inputformatClass
   * @param valueClass
   * @param indexDir
   * @param columnName
   * @throws IOException
   */
  public static void setIndexOptions(Job job, String inputformatClass,
      String valueClass, String indexDir, String columnName) throws IOException {
    setOptions(job, inputformatClass, valueClass, indexDir, null, columnName, true);
  }

  /**Set values for the underlying inputformat class.
   * We allow indexing job to have filters for debugging/testing purposes.
   * Thus relying on whether there is a filter to determine
   * if it is a indexing or searching job is not sufficient anymore.
   * @param job
   * @param inputformatClass
   * @param valueClass
   * @param indexDir
   * @param filterConditions
   * @param indexColumn
   * @param indexingFlag: true for indexing jobs; false for searching jobs.
   */

  private static void setOptions(Job job, String inputformatClass,
      String valueClass, String indexDir, String filterConditions,
      String indexColumn, boolean indexingFlag) {
    Configuration conf = job.getConfiguration();
    conf.set(REALINPUTFORMAT, inputformatClass);
    conf.set(VALUECLASS, valueClass);
    conf.set(INDEXDIR, indexDir);
    if (filterConditions != null)
      conf.set(FILTERCONDITIONS, filterConditions);
    if (indexColumn != null)
      conf.set(COLUMNNAME, indexColumn);
    conf.setBoolean(INDEXINGJOBFLAG, indexingFlag);
  }

  public void setSearchFilter(BinaryExpression filter) {
    this.filter = filter;
  }

  /**
   * @param context
   * @return the index directory provided to the job
   */
  public static String getIndexDir(JobContext context) {
    return context.getConfiguration().get(INDEXDIR);
  }

/**
 * Returns a index scheme-specific implementation of IndexedFilterRecordReader
 * (this essentially amounts to creating an IndexedFilterRecordReader that has the right
 * getFilterRecordReader, so that filters get applied to indexes correctly.
 * @param filter
 * @param colunName2Method
 * @return
 */
  protected IndexedFilterRecordReader<K, V> getIndexedFilterRecordReader(BinaryExpression filter,
      HashMap<String, Method> columnName2Method) {
    return new IndexedFilterRecordReader<K, V>(filter, columnName2Method);
  }

  /**
   * @param context
   * @param file
   *          an input file to work on provided to the job
   * @return true if there is a index file for the input file
   * @throws IOException
   */
  public static boolean foundIndexFile(JobContext context, Path file)
      throws IOException {

    Configuration conf = context.getConfiguration();
    FileSystem fs = file.getFileSystem(conf);
    Path indexFilePath = new Path(getIndexDir(context)
        + file.toUri().getRawPath() + "/"
        + BlockIndexedFileInputFormat.INDEXMETAFILENAME);
    if (!fs.exists(indexFilePath)) {
      LOG.info("no index file found for input file:" + file);
      return false;
    }
    FSDataInputStream in = fs.open(indexFilePath);

    ThriftWritable<FileIndexDescriptor> writable = ThriftWritable
        .newInstance(FileIndexDescriptor.class);
    writable.readFields(in);
    FileIndexDescriptor indexDescriptor = writable.get();
    in.close();
    return verifyInputFileCheckSum(indexDescriptor, context);
  }

  /**
   * @param indexDescriptor
   * @param context
   * @return true if the current version of the base file's checksum
   * matches what was stored in the indexDescriptor.
   * @throws IOException
   */
  protected static boolean verifyInputFileCheckSum(
      FileIndexDescriptor indexDescriptor, JobContext context)
          throws IOException {

    Configuration conf = context.getConfiguration();
    FileSystem fs = FileSystem.get(conf);

    Path file = new Path(indexDescriptor.getSourcePath());
    FileChecksum oldChecksum = indexDescriptor.getChecksum();

    // check InputFile Checksum.
    org.apache.hadoop.fs.FileChecksum cksum = fs.getFileChecksum(file);
    if (cksum != null) {
      FileChecksum newCksum = new FileChecksum(cksum.getAlgorithmName(),
          ByteBuffer.wrap(cksum.getBytes()), cksum.getLength());
      return (newCksum.equals(oldChecksum));
    }
    return true;
  }

  /**
   * @param context
   * @param file
   *          the input file provided to the job to work on
   * @param splitMaxSize any returned block's size cannot exceed this.
   * @return the list of indexed blocks which may contain rows matching the
   * filter conditions set in the job configuration.
   * @throws IOException
   */
  public List<LongPairWritable> getMatchedSplits(JobContext context, Path file, long splitMaxSize)
      throws IOException {
    BinaryExpression filter = getFilterCondition(context);
    return getFilterQualifiedBlocks(context, file, filter, splitMaxSize);
  }

  private List<LongPairWritable> getFilterQualifiedBlocks(JobContext context,
      Path file, BinaryExpression filterCondition, long splitMaxSize) throws IOException {

    Expression lhs = filterCondition.getLhs();
    Expression rhs = filterCondition.getRhs();

    if (filterCondition.getOpType() == OpType.OP_EQ) { // "leaf node"
      // handle cases like 'abcd' == column , column == 'abcd'
      if (rhs instanceof Column && lhs instanceof Const) {
        lhs = filterCondition.getRhs();
        rhs = filterCondition.getLhs();
      }
      String columnName = ((Column) lhs).getName();
      String value = ((String) ((Const) rhs).getValue());
      Text searchedValue = new Text(value);

      FileStatus[] dirlist = listIndexFiles(context, file, columnName);
      int part_num = dirlist.length;
      int part_seqnum = (new HashPartitioner<Text, Text>()).getPartition(
          searchedValue, searchedValue, part_num);
      String part_name = "/part-r-" + String.format("%05d", part_seqnum);
      FileSystem fs = file.getFileSystem(context.getConfiguration());
      MapFile.Reader mapFileIndexReader = new MapFile.Reader(fs,
          getIndexDir(context) + file.toUri().getRawPath() + "/" + columnName
          + part_name, context.getConfiguration());
      ListLongPair indexedBlocks = new ListLongPair();
      mapFileIndexReader.get(searchedValue, indexedBlocks);
      mapFileIndexReader.close();
      return indexedBlocks.get();
    }

    List<LongPairWritable> blocksLeft = getFilterQualifiedBlocks(context, file,
        (BinaryExpression) lhs, splitMaxSize);
    List<LongPairWritable> blocksRight = getFilterQualifiedBlocks(context, file,
        (BinaryExpression) rhs, splitMaxSize);

    if (filterCondition.getOpType() == OpType.OP_AND)
      return andFilter(blocksLeft, blocksRight);
    else if (filterCondition.getOpType() == OpType.OP_OR){
      return orFilter(blocksLeft, blocksRight, splitMaxSize);
    }
    else
      throw new IOException("not supported filter condition:" + filterCondition);
  }

  /** This function returns a list of sorted blocks from unioning the two input lists of blocks.
   * The returned result does not contain any overlapping bllocks. <p>
   *
   * Because the indexing jobs might have combined multiple input blocks to a
   * bigger block for each keyword to be unioned, we need to consider
   * the overlapping cases where one indexed block for one keyword "overlaps"
   * with an indexed block for the other keyword.<p>
   *
   * Essentially what we should do here is to merge the two input lists of
   * sorted "ranges" where ranges from the two input lists can overlap.
   * Logically a straightforward solution has two steps: <p>
   * 1) first merge sort both input lists based on the start offset of each
   * range (block);<br>
   * 2) then go over the merged list from the beginning to the end,
   *  combine any adjacent blocks which overlap, and return the
   *  combined list.<p>

   * Instead of going the input lists twice, in the implementation we optimize
   * by combining the two steps (sorting and combining) together,
   * thus go over the two input splits once.<p>
   *
   * We scan the two input lists and check if the two head blocks overlap or
   * not. <br>
   * A) if no overlapping the head block with the smaller end offset will
   * added to the result and we advance to its next block; <br>
   * B) if there is overlap, we compute the unioned block <br>
   *  as the new "head block" and advance the other list whose head block
   *   had the smaller endoffset.<p>
   *
   * Also we need to check if the size of the unioned block is more than a
   *  threshold. if so this overlapping case is treated as non-overlapping
   *  (see more details at the end). <br>
   * We repeat the above process until at least input is exhausted.<p>
   *
   * We union two blocks don't create a unioned block whose size is larger<
   * than a threshold to impact parallelism though the goal of using indexing is
   * to reduce the number of mappers. <p>
   *
   * For example, the following case can happen:
   * both input lists contain a list of large number of small blocks, and the
   * lists highly overlap, the end result could be that we end up creating
   * just a single block starting from the very beginning to the end of
   * the input file. We avoid this by checking against a block size threshold:
   * the splitMaxSize parameter.
   *
   * @param blocksLeft : sorted list of non-overlapping LongPair objects.
   * @param blocksRight: sorted list of non-overlapping LongPair objects.
   * @param splitMaxSize: the max size of a combined block we can create.
   * @return the sorted list of unioned LongPair objects from the the inputs.
   *
   */
  private List<LongPairWritable> orFilter(List<LongPairWritable> blocksLeft,
      List<LongPairWritable> blocksRight, long splitMaxSize) {

    if (blocksRight.size() == 0)
      return blocksLeft;

    if (blocksLeft.size() == 0)
      return blocksRight;


    List<LongPairWritable> results = new ArrayList<LongPairWritable>();

    //both lists are not empty
    Iterator<LongPairWritable> i1 = blocksLeft.iterator();
    Iterator<LongPairWritable> i2 = blocksRight.iterator();

    LongPairWritable left = i1.next();
    LongPairWritable right = i2.next();
    LongPairWritable t = null;


    while (left != null && right != null) {
      if (left.getFirst() >= right.getSecond()) {
        //non-overlapping case,  the right block is before the left block
        results.add(right);
        right = i2.hasNext() ? i2.next() : null;
      }
      else if ( right.getFirst() >= left.getSecond()) {
        //non-overlapping case, the left block is before the right block
        results.add(left);
        left = i1.hasNext() ? i1.next() : null;
      }
      else { //overlapping case
        long max = Math.max(left.getSecond(), right.getSecond());
        long min = Math.min(left.getFirst(), right.getFirst());
        //always true max > min
        if ( max - min > splitMaxSize) {
          // over threshold size, won't combine.
          long midPoint = Math.min(left.getSecond(), right.getSecond());
          //always true midPoint > min
          results.add(new LongPairWritable(min, midPoint));
          if (midPoint == max) {// both blocks have the same endoffset.
            left = i1.hasNext() ? i1.next() : null;
            right = i2.hasNext() ? i2.next() : null;
            continue;
          }
          else
            t = new LongPairWritable(midPoint, max);
        }
        else // can combine, t is the combined block, no appending to results now
          t = new LongPairWritable(min, max);

        //advance one input list
        if (right.getSecond() >= left.getSecond()) {
          right = t;
          left = i1.hasNext() ? i1.next() : null;
        }
        else {
          left = t;
          right = i2.hasNext() ? i2.next() : null;
        }
      }
    }

    if (left != null) {
      results.add(left);
      while(i1.hasNext())
        results.add(i1.next());
    }

    if (right != null) {
      results.add(right);
      while(i2.hasNext())
        results.add(i2.next());
    }
    return results;
  }

  /**This function returns a sorted list of sorted blocks from 'intersecting' the blocks
   * from the two input lists. The result contains no overlapping blocks<p>
   *
   * Because indexing jobs might have combined multiple input blocks to a large
   * block in the index file, two blocks from the two input lists could overlap,
   * and also a block from one input list could make a few  blocks from the
   * other list (part of them) eligible as results. <p>
   *
   * Java Collection.removeAll, addAll, retainAll don't work here since it
   * uses == in containment determination,  and it uses 2 loops O(n^2). <p>
   *
   * We scan the two input lists only once. We ignore blocks from both lists
   * until we find two overlapping blocks from the input lists. The overlapped
   * part is added to the results. The non-overlapping part after the<br>
   * overlapping part of the two blocks should be carried over since it might
   * overlap with the next smaller block from the input lists.  We repeat the
   * above process until one input list is exhausted.
   *
   * @param blocksLeft : sorted list of non-overlapping LongPair objects.
   * @param blocksRight: sorted list of non-overlapping LongPair objects.
   * @return the sorted list of "Anded" LongPair objects from the the inputs.
   * The result contains no duplicate values.
   */
  private List<LongPairWritable> andFilter(List<LongPairWritable> blocksLeft,
      List<LongPairWritable> blocksRight) {

    List<LongPairWritable> results = new ArrayList<LongPairWritable>();
    if (blocksLeft.size() == 0 || blocksRight.size() == 0)
      return results;

    Iterator<LongPairWritable> i1 = blocksLeft.iterator();
    Iterator<LongPairWritable> i2 = blocksRight.iterator();

    LongPairWritable left = i1.next();
    LongPairWritable right = i2.next();

    while (left !=null && right !=null) {
      if (left.getFirst() >= right.getSecond()) { //non-overlapping case
        right = i2.hasNext() ? i2.next() : null;
      }
      else if ( right.getFirst() >= left.getSecond())  {//non-overlapping case
        left = i1.hasNext() ? i1.next() : null;
      }
      else { //overlapping case
        results.add(new LongPairWritable(Math.max(left.getFirst(), right.getFirst()),
            Math.min(left.getSecond(), right.getSecond())));
        /* We don't need to consider
         * the 5 fine cases of how left and right blocks overlap.
         * Particularly we do not need to handle the case where we want to
         * move both left and right to the next elements. The next iteration
         * will take care of this since both input lists are sorted
         * and each is non-overlapping. Also we don't have to
         * really compute left/right to be the partial block after the
         * overlapped part which won't gain any performance.
         */
        if (left.getSecond() >= right.getSecond()) {
          right = i2.hasNext() ? i2.next() : null;
        } else {
          left = i1.hasNext() ? i1.next() : null;
        }
      }
    }
    return results;
  }

  /**
   * @param context
   * @param file
   *          the input file provided to the job to work on
   * @param columnName
   * @return the list of index files if there is an index directory created for
   *         the input file
   * @throws IOException
   */
  protected static FileStatus[] listIndexFiles(JobContext context, Path file,
      String columnName) throws IOException {

    Path indexFilePath = new Path(getIndexDir(context)
        + file.toUri().getRawPath() + "/" + columnName);

    FileSystem fs = file.getFileSystem(context.getConfiguration());
    FileStatus[] dirlist = fs.listStatus(indexFilePath, indexFileFilter);
    return dirlist;
  }

  private final static PathFilter indexFileFilter = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      return path.getName().startsWith("part-");
    }
  };

  /**
   * for indexing jobs which needs to read all data, we just use the real
   * RecordReader returned by the real InputFormat class; <p>
   *
   * for searching jobs,
   * we have to use a "wrapped" RecorderReader to do filtering ourself
   * since we are indexing at Block level, not Record/Line level. <p>
   *
   * Also Pig would not do the filtering once filtering conditions are
   * pushed to data sources.
   * */
  @Override
  public RecordReader<K, V> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    RecordReader<K, V> realRecorderReader;
    realRecorderReader = getRealRecordReader(split, context);

    if (getFilterCondition(context) != null) {
      String valueClass = context.getConfiguration().get(
          BlockIndexedFileInputFormat.VALUECLASS);

      BinaryExpression filter = getFilterCondition(context);
      HashMap<String, Method> colunName2Method = new HashMap<String, Method>();
      Set<String> allColumns = getAllColumnNames(filter);
      for (String colName : allColumns) {
        Method method = null;
        Class<?> c = getValueClassByName(valueClass);
        String methodName;
        methodName = getCamelCaseMethodName(colName, c);
        try {
          method = c.getMethod(methodName);
          colunName2Method.put(colName, method);
        } catch (Exception e) {
          throw new IOException("couldn't get Method from columname", e);
        }
      }
      LOG.info("using IndexedFilterRecordReader, filter:" + filter);
      return getIndexedFilterRecordReader(filter, colunName2Method);
    } else
      // use the real RecordReader to read everything
      return realRecorderReader;
  }



  /**
   * @return the list of column names used in the filter condition.
   */
  protected static Set<String> getAllColumnNames(BinaryExpression filter) {
    Set<String> results = new HashSet<String>();
    if (filter.getOpType() == Expression.OpType.OP_EQ) { // "leaf node"
      Column col = (Column) ((filter.getLhs() instanceof Column) ? filter
          .getLhs() : filter.getRhs());
      results.add(col.getName());
      return results;
    } else {
      Set<String> result1 = getAllColumnNames((BinaryExpression) filter
          .getLhs());
      Set<String> result2 = getAllColumnNames((BinaryExpression) filter
          .getRhs());
      results.addAll(result1);
      results.addAll(result2);
      return results;
    }
  }

  /**
   * It return the right value class based on the input string name.
   * Protocol buffer classes uses inner classes and treated accordingly.
   */
  public static Class<?> getValueClassByName(String valueClassName) {
    Class<?> c;
    try {
      c = Class.forName(valueClassName);
    } catch (ClassNotFoundException e) {
      // it can be a protocol buffer inner class
      c = Protobufs.getProtobufClass(valueClassName);
    }
    return c;
  }

  /**
   * return the right method name based on the input column name.<br>
   *  Works with both Thrift and Protocol classes.
   */
  public static String getCamelCaseMethodName(String columnName, Class<?> c) {
    if (TBase.class.isAssignableFrom(c))
      return "get" + columnName.substring(0, 1).toUpperCase()
          + columnName.substring(1);
    else if (Message.class.isAssignableFrom(c)) {
      return "get"
          + CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, columnName);
    }
    throw new RuntimeException(
        "only Thrift and Protocol Buffer value classes are supported");
  }

  /**
   * return the real RecordReader based on inputformat and value classes <br>
   * information stored in the job's configuration.
   */
  public RecordReader<K, V> getRealRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {

    String inputformat = context.getConfiguration().get(
        BlockIndexedFileInputFormat.REALINPUTFORMAT);
    String valueClass = context.getConfiguration().get(
        BlockIndexedFileInputFormat.VALUECLASS);

    FileInputFormat<K, V> realInputFormat = getInputFormatClass(inputformat,
        valueClass);
    return realInputFormat.createRecordReader(split, context);
  }

  /**
   * return the real InputFormat class based on inputformat <br>
   *  and value classes information parameters.
   */
  @SuppressWarnings("unchecked")
  protected FileInputFormat<K, V> getInputFormatClass(String inputFormatName,
      String valueClassName) throws IOException {
    FileInputFormat<K, V> realInputFormat = null;

    try {
      Class<? extends FileInputFormat<K, V>> inputformtClass;
      TypeRef<V> typeRef = null;
      inputformtClass = (Class<? extends FileInputFormat<K, V>>) Class
          .forName(inputFormatName);
      Class<?> c = getValueClassByName(valueClassName);
      typeRef = new TypeRef<V>(c) {
      };
      Constructor<? extends FileInputFormat<K, V>> constructor;
      constructor = inputformtClass.getConstructor(TypeRef.class);
      realInputFormat = constructor.newInstance(typeRef);
    } catch (Exception e) {
      LOG.error("could not instantiate real InputFormat from "
          + inputFormatName + "," + valueClassName);
      throw new IOException(e);
    }
    return realInputFormat;
  }

  protected boolean noFilterCondition(JobContext context) {
    return context.getConfiguration().get(FILTERCONDITIONS) == null;
  }

  protected BinaryExpression getFilterCondition(JobContext context)
      throws IOException {
    if (filter != null) {
      return filter;
    }
    String filterString = context.getConfiguration().get(FILTERCONDITIONS);
    if (filterString == null) {
      return null;
    }
    return com.twitter.elephanttwin.retrieval.Expression.
        getFilterCondition(filterString);
  }

  /*
   * Searching jobs have filters. <br>
   * We also allow indexing job to have a filter for debugging/testing purposes.
   */
  protected boolean isIndexingJob(JobContext context) {
    return context.getConfiguration().getBoolean(INDEXINGJOBFLAG, true);
  }
}