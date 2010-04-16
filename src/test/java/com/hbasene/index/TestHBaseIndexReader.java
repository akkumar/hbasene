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
package com.hbasene.index;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestHBaseIndexReader extends AbstractHBaseneTest {

  @Test
  public void testSearch() throws IOException {
    IndexSearcher searcher = new IndexSearcher(this.indexReader);
    TopDocs docs = searcher.search(new TermQuery(new Term("content", "plays")),
        3);
    Assert.assertTrue(docs.totalHits > 3,
        "At least 3 terms with the keyword plays available");
    for (ScoreDoc scoreDoc : docs.scoreDocs) {
      Assert.assertTrue(scoreDoc.doc >= 0, "Doc Id  " + scoreDoc.doc
          + " is >= 0"); // valid
      // docId
      Assert.assertTrue(scoreDoc.score > 0.0f, "Score " + scoreDoc.score
          + " > 0.0f"); // valid
      // Score

      try {
        Assert.assertNotNull(this.indexReader.document(scoreDoc.doc),
            "Retrieving document for " + scoreDoc.doc);
      } catch (Exception ex) {
        Assert.assertTrue(false,
            "Exception occurred while retrieving document for " + scoreDoc.doc
                + " " + ex);
      }

      // valid document
    }
    Document doc = this.indexReader.document(docs.scoreDocs[0].doc);
    Assert.assertEquals("FourthTimes", doc.get(PK_FIELD));
    // maximum # of plays - hence expecting it to be top-most rank.
  }
}
