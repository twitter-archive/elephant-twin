package com.twitter.elephanttwin.verification;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import com.twitter.elephanttwin.io.ListLongPair;
import com.twitter.elephanttwin.retrieval.BlockIndexedFileInputFormat;
import com.twitter.elephanttwin.retrieval.Expression.BinaryExpression;
import com.twitter.elephanttwin.retrieval.Expression.Column;
import com.twitter.elephanttwin.retrieval.Expression.Const;
import com.twitter.elephanttwin.retrieval.Expression.OpType;

/**
 * Internal tool for testing indexing jobs. Not used in indexing jobs.
 * <p>
 * The mapper is used to search rows based on key values
 * in an index file. Instead of full table scan, it reads the Mapfile index data
 * and manually constructs a list of InputSplits and a RecordReader
 * for each unique key in the index file. <p>
 * It repeatedly drives the RecordReader to  count the number of times
 * each key appears in the base input file which has been indexed.
 */
public abstract class IndexScanMapper extends
    Mapper<Text, ListLongPair, Text, LongWritable> {

  public static String searchColumnName ="indexscan.columnname";

  protected static Logger LOG = Logger.getLogger(IndexScanMapper.class);
  String baseFileName; // the input file name.
  String columnName;


  @Override
  protected void setup(Context context) throws IOException,
      InterruptedException {

    /** compute the base input file name based on the index data's filepath
     */

    String indexdir = BlockIndexedFileInputFormat.getIndexDir(context);
    FileSplit split = (FileSplit) context.getInputSplit();
    String indexdatapath = split.getPath().toUri().getPath();
    int start = (indexdir.charAt(indexdir.length() - 1) == '/') ? indexdir
        .length() - 1 : indexdir.length();
    if (indexdatapath.charAt(indexdatapath.length() - 1) == '/')
      indexdatapath = indexdatapath.substring(0, indexdatapath.length() - 1);
    String s1 = indexdatapath.substring(0, indexdatapath.lastIndexOf('/'));
    String s2 = s1.substring(0, s1.lastIndexOf('/'));

    int end = s2.lastIndexOf('/');
    baseFileName = indexdatapath.substring(start, end);
    columnName = context.getConfiguration().get(searchColumnName);
    LOG.info("baseFileName:" + baseFileName + " columnName:" + columnName);
  }

  /** for each unique key in the index file,
   * read the index blocks, compute a list of  InputSplit,
   * contruct a RecordReader, and count how many times
   * the key value appears in the base file;
   */
  @SuppressWarnings({ "rawtypes" })
  @Override
  public void map(Text key, ListLongPair value, Context context)
      throws IOException, InterruptedException {

    BlockIndexedFileInputFormat<Text, ListLongPair> inputFormat = getBlockIndexedInputFormat();
    BinaryExpression filter = new BinaryExpression( new Column(columnName),
        new Const(key.toString()), OpType.OP_EQ );
    inputFormat.setSearchFilter(filter);
    List<InputSplit> splits = inputFormat.computeSplits(context, new Path(
        baseFileName), value.get(), new String[] {}, context.getConfiguration().getInt("dfs.block.size", 256 * 1024 * 1024));

    long cnt = 0;
    for (InputSplit split : splits) {
      context.progress();
      RecordReader rr = inputFormat
          .createRecordReader(split, context);
      rr.initialize(split, context);
      while (rr.nextKeyValue()) {
        cnt++;
      }
      rr.close();
    }
    context.write(key, new LongWritable(cnt));
    return;
  }

  protected abstract BlockIndexedFileInputFormat<Text, ListLongPair> getBlockIndexedInputFormat();
}