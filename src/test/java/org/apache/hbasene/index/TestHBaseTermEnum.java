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

public class TestHBaseTermEnum extends AbstractHBaseneTest {

  @SuppressWarnings("unused")
  private static final Logger LOGGER = Logger.getLogger(TestHBaseTermEnum.class
      .getName());

  private static HBaseTermEnum termEnum;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUp() throws Exception {
    HBaseIndexReader indexReader = new HBaseIndexReader(conf, TEST_INDEX);
    termEnum = new HBaseTermEnum(indexReader);

  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDown() throws Exception {
    termEnum.close();
  }

  @Test
  public void testTermEnum() throws CorruptIndexException,
      LockObtainFailedException, IOException {
    while (termEnum.next()) {
      Term term = termEnum.term();
      String field = term.field();
      Assert.assertTrue(field.contains("content") || field.contains("id"));
      Assert.assertTrue("At least one document present with the given term",
          termEnum.docFreq() > 0);
    }

  }
}
