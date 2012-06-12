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
import java.util.Random;

import org.apache.hadoop.mapreduce.Mapper;

/**
 * An indexing mapper that handles sampling, which is a common feature shared by many indexing jobs.
 *
 * @author jimmy
 */
public abstract class AbstractSamplingIndexingMapper<KIN, VIN, KOUT, VOUT> extends
    AbstractIndexingMapper<KIN, VIN, KOUT, VOUT> {
  // Name of the property for specifying sampling percentage.
  public static final String SAMPLE_PERCENTAGE = "sample_percentage";

  private int samplePercentage;
  private Random rand;

  @Override
  public void setup(Mapper<KIN, VIN, KOUT, VOUT>.Context context) throws IOException {
    // Allow the user to specify a randomly-sampled percentage of statuses to index.
    // By default, index everything.
    samplePercentage = context.getConfiguration().getInt(SAMPLE_PERCENTAGE, 100);
    rand = samplePercentage == 100 ? null : new Random();
  }

  @Override
  protected boolean filter(KIN k, VIN v) {
    return samplePercentage == 100 ? true : rand.nextInt(100) < samplePercentage;
  }

}
