/**
 * Copyright 2009 The Apache Software Foundation
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
package org.apache.hbasene.index.create;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbasene.index.create.mapred.BuildTableIndex;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.FSDirectory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestBuildIndex {

  private static final Log LOG = LogFactory.getLog(TestBuildIndex.class);

  private final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final String TABLE = "buildindex-testtable";

  private static final String COLUMN = "col2";

  private static final String INDEX_DIR = System.getProperty("user.dir")
      + File.separator + "index";

  private HBaseAdmin admin;

  @BeforeClass
  public void setUpBeforeClass() throws Exception {
    FileUtils.deleteDirectory(new File(INDEX_DIR));
    TEST_UTIL.startMiniCluster(1);
    // TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval",
    // 100);
    // TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    // TEST_UTIL.getConfiguration().setInt("hbase.client.retries.number", 6);
  }

  @AfterClass
  public void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @BeforeMethod
  public void setUp() throws Exception {
    this.admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    HTableDescriptor[] tables = admin.listTables();
    int numTables = tables.length;
    HTable ht = TEST_UTIL.createTable(Bytes.toBytes(TABLE),
        HConstants.CATALOG_FAMILY);
    tables = this.admin.listTables();
    Assert.assertEquals(numTables + 1, tables.length);

    this.admin.disableTable(TABLE);
    try {
      new HTable(TEST_UTIL.getConfiguration(), TABLE);
    } catch (org.apache.hadoop.hbase.client.RegionOfflineException e) {
      // Expected
    }
    this.admin.addColumn(TABLE, new HColumnDescriptor(COLUMN));
    this.admin.enableTable(TABLE);

    // Add some elements.
    final byte[] row1 = Bytes.toBytes("row1");
    Put put = new Put(row1);
    put.add(HConstants.CATALOG_FAMILY, Bytes.toBytes(COLUMN), Bytes
        .toBytes("SFO"));
    ht.put(put);

    final byte[] row2 = Bytes.toBytes("row2");
    Put put2 = new Put(row2);
    put2.add(HConstants.CATALOG_FAMILY, Bytes.toBytes(COLUMN), Bytes
        .toBytes("SJC"));
    ht.put(put2);

    final byte[] row3 = Bytes.toBytes("row3");
    Put put3 = new Put(row3);
    put3.add(HConstants.CATALOG_FAMILY, Bytes.toBytes(COLUMN), Bytes
        .toBytes("OAK"));
    ht.put(put3);

    ht.flushCommits();
  }

  @Test
  public void testBuildIndex() throws IOException, ParseException {
    final int MAX_RESULTS = 10;
    BuildTableIndex build = new BuildTableIndex();

    String[] args = new String[] { "-m", "2", "-r", "1", "-indexDir",
        INDEX_DIR, "-table", TABLE, "-columns", HConstants.CATALOG_FAMILY_STR };
    build.run(args);

    // Do some search.
    IndexSearcher searcher = new IndexSearcher(FSDirectory.open(new File(
        INDEX_DIR + File.separator + "part-00000")));

    Assert.assertEquals(3, searcher.getIndexReader().numDocs(),
        "Total number of docs for searching is 3");

    searcher.close();
  }
}
