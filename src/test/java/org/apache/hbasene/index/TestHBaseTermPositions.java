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
import java.util.Map;
import java.util.NavigableMap;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbasene.index.AbstractIndexWriter;
import org.apache.hbasene.index.HBaseIndexTransactionLog;
import org.apache.hbasene.index.HBaseTermPositions;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHBaseTermPositions {

  private static final Logger LOGGER = Logger.getLogger(TestHBaseTermPositions.class
      .getName());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final String TEST_INDEX = "idx-hbase-lucene";

  private static final String PK_FIELD = "id";

  private static Configuration conf;

  private static HBaseIndexTransactionLog hbaseIndex;

  private static HBaseTermPositions termPositions;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
    conf = TEST_UTIL.getConfiguration();
    HBaseIndexTransactionLog.dropLuceneIndexTable(TEST_INDEX, conf);
    hbaseIndex = new HBaseIndexTransactionLog(conf, TEST_INDEX);

    AbstractIndexWriter writer = new AbstractIndexWriter(hbaseIndex, PK_FIELD);

    addDocument(writer, "FactTimes", "Messi plays for Barcelona");
    addDocument(writer, "UtopiaTimes", "Lionel M plays for Manchester United");
    addDocument(writer, "ThirdTimes", "Rooney plays for Manchester United");
    addDocument(writer, "FourthTimes", "Messi plays for argentina as well");

    Assert.assertTrue(new HBaseAdmin(conf).tableExists(TEST_INDEX));

    assertDocumentPresent("FactTimes");
    assertDocumentPresent("UtopiaTimes");
    assertDocumentPresent("ThirdTimes");
    assertDocumentPresent("FourthTimes");

    listAll(HBaseIndexTransactionLog.FAMILY_TERMVECTOR);
    listAll(HBaseIndexTransactionLog.FAMILY_DOCUMENTS);
    listAll(HBaseIndexTransactionLog.FAMILY_INT_TO_DOC);
    listIntQualifiers(HBaseIndexTransactionLog.FAMILY_DOC_TO_INT);
    listAll(HBaseIndexTransactionLog.FAMILY_SEQUENCE);

    assertTermVectorDocumentMapping("content/messi", 1);
    assertTermVectorDocumentMapping("content/lionel", 2);

    termPositions = new HBaseTermPositions(conf, TEST_INDEX);

  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    LOGGER.info("***   Shut down the HBase Cluster  ****");
    termPositions.close();
    HBaseIndexTransactionLog.dropLuceneIndexTable(TEST_INDEX, conf);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testTermDocs() throws CorruptIndexException,
      LockObtainFailedException, IOException {
    termPositions.seek(new Term("content", "plays"));
    int count = 0;
    while (termPositions.next()) {
      ++count;
    }
    Assert.assertEquals("plays occurs 4 ", 4, count);

  }

  static void listAll(final byte[] family) throws IOException {
    LOGGER.info("****** " + Bytes.toString(family) + "****");
    HTable table = new HTable(conf, TEST_INDEX);
    ResultScanner scanner = table.getScanner(family);
    Result result = scanner.next();
    while (result != null) {
      NavigableMap<byte[], byte[]> map = result.getFamilyMap(family);
      final StringBuilder sb = new StringBuilder();
      for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
        sb.append(" (" + Bytes.toString(entry.getKey()) + ", "
            + Bytes.toString(entry.getValue()) + ")");
      }
      LOGGER.info(Bytes.toString(result.getRow()) + sb.toString());
      result = scanner.next();
    }
    table.close();
    LOGGER.info("****** Close " + Bytes.toString(family) + "****");
  }

  static void listIntQualifiers(final byte[] family) throws IOException {
    LOGGER.info("****** " + Bytes.toString(family) + "****");
    HTable table = new HTable(conf, TEST_INDEX);
    ResultScanner scanner = table.getScanner(family);
    Result result = scanner.next();
    while (result != null) {
      NavigableMap<byte[], byte[]> map = result.getFamilyMap(family);
      final StringBuilder sb = new StringBuilder();
      for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
        sb.append(" (" + Bytes.toString(entry.getKey()) + ", "
            + Bytes.toInt(entry.getValue()) + ")");
      }
      LOGGER.info(Bytes.toString(result.getRow()) + sb.toString());
      result = scanner.next();
    }
    table.close();
    LOGGER.info("****** Close " + Bytes.toString(family) + "****");
  }

  static void addDocument(final AbstractIndexWriter writer, final String id,
      final String content) throws CorruptIndexException, IOException {
    Document doc = new Document();
    doc.add(new Field("content", content, Field.Store.NO,
        Field.Index.ANALYZED_NO_NORMS));
    doc.add(new Field("id", id, Field.Store.YES, Field.Index.NO));
    writer.addDocument(doc, new StandardAnalyzer(Version.LUCENE_30));
  }

  static void assertDocumentPresent(final String docId) throws IOException {
    Get get = new Get(Bytes.toBytes(docId));
    get.addFamily(HBaseIndexTransactionLog.FAMILY_DOCUMENTS);
    HTable table = new HTable(conf, TEST_INDEX);
    try {
      Result result = table.get(get);
      NavigableMap<byte[], byte[]> map = result
          .getFamilyMap(HBaseIndexTransactionLog.FAMILY_DOCUMENTS);
      Assert.assertTrue(map.size() > 0);
    } finally {
      table.close();
    }
  }

  /**
   * Asserts if a mapping exists between the given term and the doc Id.
   * 
   * @param term
   * @param docId
   * @throws IOException
   */
  static void assertTermVectorDocumentMapping(final String term,
      final byte[] docId) throws IOException {
    Get get = new Get(Bytes.toBytes(term));
    get.addFamily(HBaseIndexTransactionLog.FAMILY_TERMVECTOR);
    HTable table = new HTable(conf, TEST_INDEX);
    try {
      Result result = table.get(get);
      NavigableMap<byte[], byte[]> map = result
          .getFamilyMap(HBaseIndexTransactionLog.FAMILY_TERMVECTOR);
      Assert.assertTrue(map.size() > 0);
      Assert.assertNotNull(map.get(docId));
    } finally {
      table.close();
    }
  }

  /**
   * Asserts if a mapping exists between the given term and the doc Id.
   * 
   * @param term
   * @param docId
   * @throws IOException
   */
  static void assertTermVectorDocumentMapping(final String term, final int docId)
      throws IOException {
    assertTermVectorDocumentMapping(term, Bytes.toBytes(docId));
  }
}
