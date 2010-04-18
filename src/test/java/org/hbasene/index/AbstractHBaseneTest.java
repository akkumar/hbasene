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

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.util.Version;
import org.hbasene.index.HBaseIndexReader;
import org.hbasene.index.HBaseIndexStore;
import org.hbasene.index.HBaseIndexWriter;
import org.hbasene.index.HBaseneConstants;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * Abstract HBasene Test
 */
public class AbstractHBaseneTest {

  private static final Logger LOGGER = Logger
      .getLogger(AbstractHBaseneTest.class.getName());

  private final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  protected Configuration conf;

  protected static final String TEST_INDEX = "idx-hbase-lucene";

  protected static final String PK_FIELD = "id";

  protected HTablePool tablePool;

  protected HBaseIndexWriter indexWriter;

  protected HBaseIndexReader indexReader;

  protected static final int DEFAULT_POOL_SIZE = 20;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public final void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    conf = TEST_UTIL.getConfiguration();
    HBaseIndexStore.dropLuceneIndexTable(TEST_INDEX, conf);
    HBaseIndexStore.createLuceneIndexTable(TEST_INDEX, conf, true);
    this.tablePool = new HTablePool(conf, DEFAULT_POOL_SIZE);
    HBaseIndexStore hbaseIndex = new HBaseIndexStore(this.tablePool, TEST_INDEX);

    this.indexWriter = new HBaseIndexWriter(hbaseIndex, PK_FIELD);
    doInitDocs();

    this.indexReader = new HBaseIndexReader(this.tablePool, TEST_INDEX);
    doSetupDerived();

  }

  protected void doSetupDerived() throws CorruptIndexException, IOException {

  }

  protected void doInitDocs() throws CorruptIndexException, IOException {
    this.addDefaultDocuments();

  }

  @AfterClass
  public final void tearDownBase() throws IOException {
    this.indexReader.close();
    // TODO: Release indexWriter resources.
  }

  protected void addDefaultDocuments() throws CorruptIndexException,
      IOException {
    indexWriter.addDocument(this.createDocument("FactTimes",
        "Messi plays for Barcelona"), new StandardAnalyzer(Version.LUCENE_30));
    indexWriter.addDocument(this.createDocument("UtopiaTimes",
        "Lionel M plays for Manchester United"), new StandardAnalyzer(
        Version.LUCENE_30));
    indexWriter.addDocument(this.createDocument("ThirdTimes",
        "Rooney plays for Manchester United"), new StandardAnalyzer(
        Version.LUCENE_30));
    indexWriter
        .addDocument(
            this
                .createDocument(
                    "FourthTimes",
                    "Messi plays for argentina as well. He plays as a mid-fielder and plays really well."),
            new StandardAnalyzer(Version.LUCENE_30));

    Assert.assertTrue(new HBaseAdmin(conf).tableExists(TEST_INDEX));
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public void tearDownAfterClass() throws Exception {
    LOGGER.info("***   Shut down the HBase Cluster  ****");
    // this.tablePool.close();
    // TODO: HBASE-2435
    HBaseIndexStore.dropLuceneIndexTable(TEST_INDEX, conf);
    TEST_UTIL.shutdownMiniCluster();
  }

  protected Document createDocument(final String id, final String content)
      throws CorruptIndexException, IOException {
    Document doc = new Document();
    doc.add(new Field("content", content, Field.Store.NO,
        Field.Index.ANALYZED_NO_NORMS));
    doc.add(new Field("id", id, Field.Store.YES, Field.Index.NO));
    return doc;

  }

  protected void assertDocumentPresent(final String docId) throws IOException {
    Get get = new Get(Bytes.toBytes(docId));
    get.addFamily(HBaseneConstants.FAMILY_FIELDS);
    HTable table = new HTable(conf, TEST_INDEX);
    try {
      Result result = table.get(get);
      NavigableMap<byte[], byte[]> map = result
          .getFamilyMap(HBaseneConstants.FAMILY_FIELDS);
      Assert.assertTrue(map.size() > 0);
    } finally {
      table.close();
    }
  }

  protected void listAll(final byte[] family) throws IOException {
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

  protected void listLongQualifiers(final byte[] family) throws IOException {
    LOGGER.info("****** " + Bytes.toString(family) + "****");
    HTable table = new HTable(conf, TEST_INDEX);
    ResultScanner scanner = table.getScanner(family);
    Result result = scanner.next();
    while (result != null) {
      NavigableMap<byte[], byte[]> map = result.getFamilyMap(family);
      final StringBuilder sb = new StringBuilder();
      for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
        sb.append(" (" + Bytes.toString(entry.getKey()) + ", "
            + Bytes.toLong(entry.getValue()) + ")");
      }
      LOGGER.info(Bytes.toString(result.getRow()) + sb.toString());
      result = scanner.next();
    }
    table.close();
    LOGGER.info("****** Close " + Bytes.toString(family) + "****");
  }

  protected void listLongRows(final byte[] family) throws IOException {
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
      LOGGER.info(Bytes.toLong(result.getRow()) + sb.toString());
      result = scanner.next();
    }
    table.close();
    LOGGER.info("****** Close " + Bytes.toString(family) + "****");
  }

  protected void listTermVectors() throws IOException {
    final byte[] family = HBaseneConstants.FAMILY_TERMVECTOR;
    LOGGER.info("****** " + Bytes.toString(family) + "****");
    HTable table = new HTable(conf, TEST_INDEX);
    ResultScanner scanner = table.getScanner(family);
    Result result = scanner.next();
    while (result != null) {
      NavigableMap<byte[], byte[]> map = result.getFamilyMap(family);
      final StringBuilder sb = new StringBuilder();
      for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
        sb.append(" (" + Bytes.toLong(entry.getKey()) + ", "
            + Bytes.toString(entry.getValue()) + ")");
      }
      LOGGER.info(Bytes.toString(result.getRow()) + sb.toString());
      result = scanner.next();
    }
    table.close();
    LOGGER.info("****** Close " + Bytes.toString(family) + "****");
  }

  /**
   * Asserts if a mapping exists between the given term and the doc Id.
   * 
   * @param term
   * @param docId
   * @throws IOException
   */
  protected void assertTermVectorDocumentMapping(final String term,
      final byte[] docId) throws IOException {
    Get get = new Get(Bytes.toBytes(term));
    get.addFamily(HBaseneConstants.FAMILY_TERMVECTOR);
    HTable table = new HTable(conf, TEST_INDEX);
    try {
      Result result = table.get(get);
      NavigableMap<byte[], byte[]> map = result
          .getFamilyMap(HBaseneConstants.FAMILY_TERMVECTOR);
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
  protected void assertTermVectorDocumentMapping(final String term,
      final long docId) throws IOException {
    assertTermVectorDocumentMapping(term, Bytes.toBytes(docId));
  }

}
