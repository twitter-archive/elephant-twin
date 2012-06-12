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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import com.google.common.collect.Lists;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
/**
 * Tests serialization / de-serialization
 * @author Alex Levenson
 */
public class TestCountTimestampSamplesWritable {

  @Test
  public void testCountTimestampSamplesWritable() throws Exception {
    CountTimestampSamplesWritable orig =
      new CountTimestampSamplesWritable(7L, 123L, Arrays.asList(18L, 19L, 20L));

    assertEquals(orig.getCount(), 7L);
    assertEquals(orig.getEpochMilliseconds(), 123L);
    assertEquals(Arrays.asList(18L, 19L, 20L), orig.getSamples());

    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);

    orig.write(dataOut);
    CountTimestampSamplesWritable deSerialized = new CountTimestampSamplesWritable(0L, 0L, null);

    deSerialized.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));
    assertEquals(orig.getCount(), deSerialized.getCount());
    assertEquals(orig.getEpochMilliseconds(), deSerialized.getEpochMilliseconds());
    assertEquals(orig.getSamples(), deSerialized.getSamples());
  }

  @Test
  public void testCountTimestampSamplesWritableEmptySamples() throws Exception {
    CountTimestampSamplesWritable orig =
      new CountTimestampSamplesWritable(7L, 123L, Lists.<Long>newLinkedList());

    assertEquals(orig.getCount(), 7L);
    assertEquals(orig.getEpochMilliseconds(), 123L);
    assertEquals(Lists.<Long>newLinkedList(), orig.getSamples());

    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);

    orig.write(dataOut);
    CountTimestampSamplesWritable deSerialized = new CountTimestampSamplesWritable(0L, 0L, null);

    deSerialized.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));
    assertEquals(orig.getCount(), deSerialized.getCount());
    assertEquals(orig.getEpochMilliseconds(), deSerialized.getEpochMilliseconds());
    assertEquals(orig.getSamples(), deSerialized.getSamples());
  }
}