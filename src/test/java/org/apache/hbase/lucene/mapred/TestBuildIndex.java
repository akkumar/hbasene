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
package org.apache.hbase.lucene.mapred;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
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

  private final static HBaseTestingUtility TEST_UTIL;

  private static final String TABLE = "buildindex-testtable";

  private static final String COLUMN = "col2";

  private static final String INDEX_DIR = System.getProperty("user.dir")
      + File.separator + "index";

  private HBaseAdmin admin;

  static {
    TEST_UTIL = new HBaseTestingUtility();
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    FileUtils.deleteDirectory(new File(INDEX_DIR));
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

  @Test
  public void testBuildIndex() throws IOException {
    BuildTableIndex build = new BuildTableIndex();

    String[] args = new String[] { "-m", "2", "-r", "1", "-indexDir",
        INDEX_DIR, "-table", TABLE, "-columns", COLUMN };
    build.run(args);
  }

}
