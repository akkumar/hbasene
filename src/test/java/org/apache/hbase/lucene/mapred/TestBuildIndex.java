package org.apache.hbase.lucene.mapred;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBuildIndex {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private HBaseAdmin admin;

  private static final String TABLE = "buildindex-testtable";

  private static final String COLUMN = "col2";

  @Test
  public void testBuildIndex() throws IOException {
    BuildTableIndex build = new BuildTableIndex();
    String dir = System.getProperty("user.dir") + File.separator + "index";

    String[] args = new String[] { "-indexDir", dir, "-table", TABLE,
        "-columns", COLUMN };
    build.run(args);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt("hbase.client.retries.number", 6);
    TEST_UTIL.startMiniCluster(3);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }
  
  @Before
  public void setUp() throws Exception {
    this.admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    HTableDescriptor[] tables = admin.listTables();
    int numTables = tables.length;
    TEST_UTIL.createTable(Bytes.toBytes(TABLE), HConstants.CATALOG_FAMILY);
    tables = this.admin.listTables();
    assertEquals(numTables + 1, tables.length);

    this.admin.disableTable(TABLE);
    try {
      new HTable(TEST_UTIL.getConfiguration(), TABLE);
    } catch (org.apache.hadoop.hbase.client.RegionOfflineException e) {
      // Expected
    }
    this.admin.addColumn(TABLE, new HColumnDescriptor(COLUMN));
    this.admin.enableTable(TABLE);
  }

}
