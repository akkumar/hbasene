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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHBaseIndexReader extends AbstractHBaseneTest {

  private static IndexReader reader;

  @BeforeClass
  public static void setUp() {
    reader = new HBaseIndexReader(conf, TEST_INDEX);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    reader.close();
  }

  @Test
  public void testSearch() throws IOException {
    IndexSearcher searcher = new IndexSearcher(reader);
    TopDocs docs = searcher.search(new TermQuery(new Term("content", "plays")),
        3);
    Assert.assertTrue("At least 3 terms with the keyword plays available",
        docs.totalHits > 3);
    for (ScoreDoc doc : docs.scoreDocs) {
      Assert.assertTrue("Doc Id  " + doc.doc + " is >= 0", doc.doc >= 0); // valid
                                                                          // docId
      Assert.assertTrue("Score " + doc.score + " > 0.0f", doc.score > 0.0f); // valid
                                                                             // Score
    }
  }
}
