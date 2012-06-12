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
package com.twitter.elephanttwin.indexing;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

/**
 * <p>
 * The general Hadoop indexing flow is as follows: the mappers process input records, and pass onto
 * reducers, which perform the actual indexing. The number of reducers is equal to the
 * number of shards (partitions), i.e., each reducer builds an index partition independently.
 * </p>
 *
 * <p>
 * In the mapper, we provide abstractions for the ability to perform one-to-one transformation of
 * the records, as well as a predicate for filtering records.
 * </p>
 *
 * @author jimmy
 */
public abstract class AbstractIndexingMapper<KIN, VIN, KOUT, VOUT> extends Mapper<KIN, VIN, KOUT, VOUT> {
  @Override
  public void map(KIN k, VIN v, Context context) throws IOException, InterruptedException {
    // If the record passes the filter, then emit, properly transformed.
    if (filter(k, v)) {
      KOUT keyOut = buildOutputKey(k, v);
      VOUT valueOut = buildOutputValue(k, v);
      // Make sure output key-value pairs are non-null.
      if (keyOut != null && valueOut != null) {
        context.write(keyOut, valueOut);
      }
    }
  }

  /**
   * Returns true if this input key-value pair should be processed, or false otherwise.
   */
  abstract protected boolean filter(KIN k, VIN v);

  /**
   * Builds the output key from the input key-value pair.
   */
  abstract protected KOUT buildOutputKey(KIN k, VIN v);

  /**
   * Builds the output value from the input key-value pair.
   */
  abstract protected VOUT buildOutputValue(KIN k, VIN v);
}
