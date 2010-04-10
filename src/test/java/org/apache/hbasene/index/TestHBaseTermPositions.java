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

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.LockObtainFailedException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHBaseTermPositions extends AbstractHBaseneTest {

  @SuppressWarnings("unused")
  private static final Logger LOGGER = Logger
      .getLogger(TestHBaseTermPositions.class.getName());

  private static HBaseTermPositions termPositions;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUp() throws Exception {
    HBaseIndexReader indexReader = new HBaseIndexReader(conf, TEST_INDEX);
    termPositions = new HBaseTermPositions(indexReader);

  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDown() throws Exception {
    termPositions.close();
  }

  @Test
  public void testTermDocs() throws CorruptIndexException,
      LockObtainFailedException, IOException {
    termPositions.seek(new Term("content", "plays"));
    int count = 0;
    while (termPositions.next()) {
      Assert.assertTrue(termPositions.doc() > 0);
      Assert.assertTrue(termPositions.freq() > 0);
      ++count;
    }
    Assert.assertEquals("plays occurs 4 ", 4, count);

  }
}
