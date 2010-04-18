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
package org.hbasene.index.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Version;
import org.hbasene.index.AbstractHBaseneTest;
import org.hbasene.index.search.HBaseIndexSearcher;
import org.junit.Test;


public class TestHBaseIndexSearcher extends AbstractHBaseneTest {

  private static final Logger LOG = Logger
      .getLogger(TestHBaseIndexSearcher.class.getName());

  private static final String[] AIRPORTS = { "NYC", "JFK", "EWR", "SEA", "SFO",
      "OAK", "SJC" };

  private final Map<String, List<Integer>> airportMap = new TreeMap<String, List<Integer>>();

  private HBaseIndexSearcher indexSearcher;

  @Override
  protected void doSetupDerived() throws CorruptIndexException, IOException {
    this.indexSearcher = new HBaseIndexSearcher(this.indexReader);
  }

  @Override
  protected void doInitDocs() throws CorruptIndexException, IOException {
    for (int i = 100; i >= 0; --i) {
      Document doc = this.getDocument(i);
      indexWriter.addDocument(doc, new StandardAnalyzer(Version.LUCENE_30));
    }
  }

  private Document getDocument(int i) {
    Document doc = new Document();
    doc.add(new Field("id", "doc" + i, Field.Store.YES, Field.Index.NO));
    int randomIndex = (int) (Math.random() * 7.0f);
    doc.add(new Field("airport", AIRPORTS[randomIndex], Field.Store.NO,
        Field.Index.ANALYZED_NO_NORMS));
    doc.add(new Field("searchterm", Math.random() > 0.5f ? "always" : "never",
        Field.Store.NO, Field.Index.ANALYZED_NO_NORMS));
    recordRandomIndex(100 - i, randomIndex);
    return doc;
  }

  private void recordRandomIndex(final int docIndex, final int airportIndex) {
    List<Integer> docs = airportMap.get(AIRPORTS[airportIndex]);
    if (docs == null) {
      docs = new LinkedList<Integer>();
      airportMap.put(AIRPORTS[airportIndex], docs);
    }
    docs.add(docIndex);
  }

  @Test
  public void testSortFieldAsc() throws IOException {
    LOG.info(this.airportMap.toString());
    TermQuery termQuery = new TermQuery(new Term("searchterm", "always"));
    Sort sort = new Sort(new SortField("airport", SortField.STRING));
    TopDocs docs = this.indexSearcher.search(termQuery
        .createWeight(indexSearcher), null, 25, sort, false);
    LOG.info("Total results are " + docs.scoreDocs.length);
    this.printScoreDocs(docs.scoreDocs, "Sorted ");
    assertSortOrderAsc(docs.scoreDocs);

  }

  @Test
  public void testSortFieldDesc() throws IOException {
    LOG.info(this.airportMap.toString());
    TermQuery termQuery = new TermQuery(new Term("searchterm", "always"));
    Sort sort = new Sort(new SortField("airport", SortField.STRING, true));
    //sort by reverse
    TopDocs docs = this.indexSearcher.search(termQuery
        .createWeight(indexSearcher), null, 25, sort, false);
    LOG.info("Total results are " + docs.scoreDocs.length);
    this.printScoreDocs(docs.scoreDocs, "Sorted ");
    assertSortOrderDesc(docs.scoreDocs);
  }

  
  public void tistNonExistentSortField() throws IOException {
    LOG.info(this.airportMap.toString());
    IndexSearcher searcher = new IndexSearcher(this.indexReader);
    try {
      TopDocs docs = searcher.search(new TermQuery(new Term("searchterm",
          "always")), 90);
      LOG.info("Total results are " + docs.scoreDocs.length);
      this.printScoreDocs(docs.scoreDocs, "Original Order ");
      // ScoreDoc[] result = this.metaReader.sort(docs.scoreDocs, "airport1");
      // TODO: This method should throw an exception for an invalid field to be
      // sorted.

    } finally {
      searcher.close();
    }
  }

  void printScoreDocs(final ScoreDoc[] scoreDocs, final String prefix) {
    List<Integer> originalOrder = new ArrayList<Integer>();
    for (ScoreDoc scoreDoc : scoreDocs) {
      originalOrder.add(scoreDoc.doc);
    }
    LOG.info(prefix + " is " + originalOrder);
  }

  void assertSortOrderAsc(final ScoreDoc[] result) {
    Map<Integer, String> reverseMap = new HashMap<Integer, String>();
    for (final Map.Entry<String, List<Integer>> entry : this.airportMap
        .entrySet()) {
      for (final Integer docId : entry.getValue()) {
        reverseMap.put(docId, entry.getKey());
      }
    }
    String previousAirport = "000";
    for (final ScoreDoc scoreDoc : result) {
      String currentAirport = reverseMap.get(scoreDoc.doc);
      Assert.assertTrue(currentAirport + " vs " + previousAirport,
          currentAirport.compareTo(previousAirport) >= 0);
      previousAirport = currentAirport;
    }
  }

  void assertSortOrderDesc(final ScoreDoc[] result) {
    Map<Integer, String> reverseMap = new HashMap<Integer, String>();
    for (final Map.Entry<String, List<Integer>> entry : this.airportMap
        .entrySet()) {
      for (final Integer docId : entry.getValue()) {
        reverseMap.put(docId, entry.getKey());
      }
    }
    String previousAirport = "zzz";
    for (final ScoreDoc scoreDoc : result) {
      String currentAirport = reverseMap.get(scoreDoc.doc);
      Assert.assertTrue(currentAirport + " vs " + previousAirport,
          currentAirport.compareTo(previousAirport) <= 0);
      previousAirport = currentAirport;
    }
  }
}
