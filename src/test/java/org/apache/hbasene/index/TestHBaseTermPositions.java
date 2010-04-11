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
package org.apache.hbasene.index;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.lucene.index.Term;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestHBaseTermPositions extends AbstractHBaseneTest {

  @SuppressWarnings("unused")
  private static final Logger LOGGER = Logger
      .getLogger(TestHBaseTermPositions.class.getName());

  private HBaseIndexReader indexReader;

  private HBaseTermPositions termPositions;

  /**
   * @throws java.lang.Exception
   */
  @BeforeMethod
  public void setUp() throws Exception {
    indexReader = new HBaseIndexReader(conf, TEST_INDEX);
    termPositions = new HBaseTermPositions(indexReader);

  }

  /**
   * @throws java.lang.Exception
   */
  @AfterMethod
  public void tearDown() throws Exception {
    termPositions.close();
    indexReader.close();
  }
  
  @Test
  public void testTermDocs() throws IOException {
    termPositions.seek(new Term("content", "plays"));
    int count = 0;
    while (termPositions.next()) {
      Assert.assertTrue(termPositions.doc() >= 0);
      Assert.assertTrue(termPositions.freq() > 0);
      ++count;
    }
    Assert.assertEquals( 4, count, "plays occurs 4 ");
  }

  @Test
  public void testReadNormal() throws IOException {
    int[] docs = new int[4];
    int[] freqs = new int[4];
    termPositions.seek(new Term("content", "plays"));
    Assert.assertEquals(4, termPositions.read(docs, freqs));
  }

  @Test
  public void testReadOverflowDocs() throws IOException {
    int[] docs = new int[32];
    int[] freqs = new int[32];
    termPositions.seek(new Term("content", "plays"));
    Assert.assertEquals(4, termPositions.read(docs, freqs));
  }

  @Test
  public void testReadUnderflowDocs() throws IOException {
    int[] docs = new int[3];
    int[] freqs = new int[3];
    termPositions.seek(new Term("content", "plays"));
    Assert.assertEquals(3, termPositions.read(docs, freqs));
  }

  @Test
  public void testReadMultipleSplits() throws IOException {
    termPositions.seek(new Term("content", "plays"));
    int[] docs = new int[3];
    int[] freqs = new int[3];
    Assert.assertEquals(3, termPositions.read(docs, freqs));

    docs = new int[1];
    freqs = new int[1];
    Assert.assertEquals(1, termPositions.read(docs, freqs));

  }

  @Test
  public void testReadAtLastBoundary() throws IOException {
    termPositions.seek(new Term("content", "plays"));
    int[] docs = new int[3];
    int[] freqs = new int[3];
    Assert.assertEquals(3, termPositions.read(docs, freqs));

    docs = new int[1];
    freqs = new int[1];
    Assert.assertEquals(1, termPositions.read(docs, freqs));

    Assert.assertEquals(0, termPositions.read(docs, freqs));

  }

}
