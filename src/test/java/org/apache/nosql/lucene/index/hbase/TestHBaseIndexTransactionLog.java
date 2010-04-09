package org.apache.nosql.lucene.index.hbase;

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
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;
import org.apache.nosql.lucene.index.NoSqlIndexWriter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHBaseIndexTransactionLog {

  private static final Logger LOGGER = Logger
      .getLogger(TestHBaseIndexTransactionLog.class.getName());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final String TEST_INDEX = "idx-hbase-lucene";

  private static final String PK_FIELD = "id";

  private static Configuration conf;

  private static HBaseIndexTransactionLog hbaseIndex;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
    conf = TEST_UTIL.getConfiguration();
    HBaseIndexTransactionLog.dropLuceneIndexTable(TEST_INDEX, conf);
    hbaseIndex = new HBaseIndexTransactionLog(conf, TEST_INDEX);

  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    LOGGER.info("***   Shut down the HBase Cluster  ****");
    HBaseIndexTransactionLog.dropLuceneIndexTable(TEST_INDEX, conf);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testAddDocuments() throws CorruptIndexException,
      LockObtainFailedException, IOException {
    NoSqlIndexWriter writer = new NoSqlIndexWriter(hbaseIndex, PK_FIELD);

    this.addDocument(writer, "FactTimes",
        "Messi plays for Barcelona");
    this.addDocument(writer, "UtopiaTimes",
        "Lionel M plays for Manchester United");

    Assert.assertTrue(new HBaseAdmin(conf).tableExists(TEST_INDEX));

    this.assertDocumentPresent("FactTimes");
    this.assertDocumentPresent("UtopiaTimes");

    this.listAll(HBaseIndexTransactionLog.FAMILY_TERM_VECTOR);
    this.listAll(HBaseIndexTransactionLog.FAMILY_DOCUMENTS);

    this.assertTermVectorDocumentMapping("content/messi", "FactTimes");
    this.assertTermVectorDocumentMapping("content/lionel", "UtopiaTimes");

  }

  void listAll(final byte[] family) throws IOException {
    LOGGER.info("****** " + Bytes.toString(family) + "****");
    HTable table = new HTable(conf, TEST_INDEX);
    ResultScanner scanner = table.getScanner(family);
    Result result = scanner.next();
    while (result != null) {
      NavigableMap<byte[], byte[]> map = result.getFamilyMap(family);
      for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
        LOGGER.info(Bytes.toString(result.getRow()) + "("
            + Bytes.toString(entry.getKey()) + ", "
            + Bytes.toString(entry.getValue()) + ")");
      }
      result = scanner.next();
    }
    table.close();
    LOGGER.info("****** Close " + Bytes.toString(family) + "****");
  }

  void addDocument(final NoSqlIndexWriter writer, final String id,
      final String content) throws CorruptIndexException, IOException {
    Document doc = new Document();
    doc.add(new Field("content", content, Field.Store.NO,
        Field.Index.ANALYZED_NO_NORMS));
    doc.add(new Field("id", id, Field.Store.YES, Field.Index.NO));
    writer.addDocument(doc, new StandardAnalyzer(Version.LUCENE_30));
  }

  void assertDocumentPresent(final String docId) throws IOException {
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
  void assertTermVectorDocumentMapping(final String term, final String docId)
      throws IOException {
    Get get = new Get(Bytes.toBytes(term));
    get.addFamily(HBaseIndexTransactionLog.FAMILY_TERM_VECTOR);
    HTable table = new HTable(conf, TEST_INDEX);
    try {
      Result result = table.get(get);
      NavigableMap<byte[], byte[]> map = result
          .getFamilyMap(HBaseIndexTransactionLog.FAMILY_TERM_VECTOR);
      Assert.assertTrue(map.size() > 0);
      Assert.assertNotNull(map.get(Bytes.toBytes(docId)));
    } finally {
      table.close();
    }
  }
}
