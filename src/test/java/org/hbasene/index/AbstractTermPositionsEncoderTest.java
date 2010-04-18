/**
 * Copyright 2010 Karthik Kumar
 * 
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hbasene.index;

import java.util.ArrayList;
import java.util.List;

import org.hbasene.index.AbstractTermPositionsEncoder;
import org.junit.Assert;
import org.testng.annotations.BeforeTest;

/**
 * Rudimentary test case of testing the encode and the decode arrays, 
 * to be used by derived classes.
 */
public abstract class AbstractTermPositionsEncoderTest {

  protected AbstractTermPositionsEncoder encoder;

  protected abstract AbstractTermPositionsEncoder createEncoder();

  protected int[] termPositions;

  @BeforeTest
  public void setup() {
    this.encoder = this.createEncoder();
    this.termPositions = new int[] { 1, 3, 4, 9, 10 };
  }

  protected void assertEncodeDecode(final int[] inputTermPositions) {
    List<Integer> input = new ArrayList<Integer>(inputTermPositions.length);
    for (int i : inputTermPositions) {
      input.add(i);
    }
    byte[] array = this.encoder.encode(input);
    int[] result = this.encoder.decode(array);
    Assert.assertArrayEquals(result, inputTermPositions);
  }


}
