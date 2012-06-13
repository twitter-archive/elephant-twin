package com.twitter.elephanttwin.indexing;

import org.apache.hadoop.mapreduce.Mapper;

import com.twitter.elephanttwin.io.LongPairWritable;
import com.twitter.elephanttwin.io.TextLongPairWritable;

/**
 * Just a bit of sugar to ensure that implementations of mappers that wish to use the block
 * indexing classes implement the right K/V types.
 * <p>
 * Output key is a pair of {column_value, [offset start, offset end]} (these offsets don't have to be original blocks!)<br>
 * Output value is the pair of [offset_start, offset_end] (as in the key).<p>
 * We have to do it twice to get secondary sorting to work in Hadoop.
 * @param <M> the writable.
 */
public abstract class BlockIndexingMapper<KIN, VIN> extends
Mapper<KIN, VIN, TextLongPairWritable, LongPairWritable>
{}
