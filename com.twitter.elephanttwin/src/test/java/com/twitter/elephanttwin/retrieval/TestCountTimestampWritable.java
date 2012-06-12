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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests serialization / de-serialization
 * @author Alex Levenson
 */
public class TestCountTimestampWritable {
  @Test
  public void testCountTimestampWritable() throws Exception {
    CountTimestampWritable orig = new CountTimestampWritable(7L, 123L);

    assertEquals(orig.getCount(), 7L);
    assertEquals(orig.getEpochMilliseconds(), 123L);

    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);

    orig.write(dataOut);

    CountTimestampWritable deSerialized = new CountTimestampWritable(0L, 0L);
    deSerialized.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

    assertEquals(orig.getCount(), deSerialized.getCount());
    assertEquals(orig.getEpochMilliseconds(), deSerialized.getEpochMilliseconds());
  }
}
