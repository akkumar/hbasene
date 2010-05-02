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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.lucene.HBaseneUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.util.OpenBitSet;

/**
 * An index formed on-top of HBase. This requires a table with predefined column
 * families.
 * 
 * <ul>
 * <li>fm.termVector</li>
 * <li>fm.fields</li>
 * </ul>
 * 
 * To create a HBase Table, specific to the index schema, refer to
 * {@link #createLuceneIndexTable(String, HBaseConfiguration, boolean)} .
 */
public class HBaseIndexStore extends AbstractIndexStore implements
    HBaseneConstants {

  private static final Log LOG = LogFactory.getLog(HBaseIndexStore.class);

  private final HTablePool tablePool;

  private final String indexName;

  private int termVectorBufferSize;

  private int termVectorArrayThreshold;

  private long docBase = 0;

  private long currentTermBufferSize = 0;

  private final Map<String, Object> termDocs = new HashMap<String, Object>();

  private long segmentId = -1;

  private long lastDocId = -1;

  /**
   * For maximum throughput, use a single table.
   */
  private final HTable termVectorTable;

  /**
   * Encoder of termPositions
   */
  // TODO: Better encoding rather than the integer form is needed.
  // Use OpenBitSet preferably again, for term frequencies
  private final AbstractTermPositionsEncoder termPositionEncoder = new AlphaTermPositionsEncoder();

  public HBaseIndexStore(final HTablePool tablePool,
      final HBaseConfiguration configuration, final String indexName)
      throws IOException {
    this.tablePool = tablePool;
    this.indexName = indexName;
    this.termVectorTable = tablePool.getTable(this.indexName);
    this.termVectorBufferSize = configuration.getInt(
        HBaseneConfiguration.CONF_MAX_TERM_VECTOR, 5 * 1000 * 1000);

    this.termVectorArrayThreshold = configuration.getInt(
        HBaseneConfiguration.CONF_TERM_VECTOR_LIST_THRESHOLD, 100);

    this.doIncrementSegmentId();
    this.initDocBase();
  }

  void initDocBase() throws IOException {
    this.docBase = Bytes.toLong(this.getCellValue(ROW_SEQUENCE_ID,
        FAMILY_SEQUENCE, QUALIFIER_SEQUENCE));
  }

  @Override
  public void close() throws IOException {
    commit();
  }

  @Override
  public void commit() throws IOException {
    this.doCommit();
    // TODO: Close all tables in the tablepool
    HTable table = this.tablePool.getTable(this.indexName);
    try {
      table.close();
    } finally {
      this.tablePool.putTable(table);
    }
  }

  @Override
  public void addTermPositions(long docId,
      final Map<String, List<Integer>> termPositionVector) throws IOException {

    //byte[] fieldTermBytes = Bytes.toBytes(fieldTerm);
    //byte[] docIdBytes = Bytes.toBytes(docId);
    //Put put = new Put(fieldTermBytes);
    //put.add(FAMILY_TERMPOSITIONS, docIdBytes, this.termPositionEncoder
   //     .encode(termPositionVector));
    //put.setWriteToWAL(false);// Do not write to WAL, since it would be very
    // expensive.
    // TODO: Add this put appropriate to the table.
    doAddDocToTerms(termPositionVector.keySet(), docId);

  }

  synchronized void doAddDocToTerms(final Set<String> fieldTerms,
      final long docId) throws IOException {
    for (final String fieldTerm : fieldTerms) {
      Object docs = this.termDocs.get(fieldTerm);
      if (docs == null) {
        docs = new ArrayList<Integer>();
        this.termDocs.put(fieldTerm, docs);
      }
      long relativeId = docId - docBase;
      if (docs instanceof List) {
        List<Integer> listImpl = (List<Integer>) docs;
        listImpl.add((int) relativeId);
        this.currentTermBufferSize += Bytes.SIZEOF_INT;
        if (listImpl.size() > this.termVectorArrayThreshold) {
          OpenBitSet bitset = new OpenBitSet();
          for (Integer value : listImpl) {
            bitset.set(value);
          }
          listImpl.clear();
          this.termDocs.put(fieldTerm, bitset);

          this.currentTermBufferSize -= (this.termVectorArrayThreshold * Bytes.SIZEOF_INT);
          this.currentTermBufferSize += (bitset.getNumWords() * Bytes.SIZEOF_LONG);
        }
      } else if (docs instanceof OpenBitSet) {
        ((OpenBitSet) docs).set(relativeId);
      }
    }
    if (this.currentTermBufferSize > this.termVectorBufferSize) {
      this.doCommit();
    }
    this.lastDocId = docId;
  }

  private void doCommit() throws IOException {
    doFlushCommitTermDocs();
    this.termDocs.clear();
    this.currentTermBufferSize = 0;
    this.docBase = this.lastDocId;
  }

  private void doFlushCommitTermDocs() throws IOException {
    int sz = this.termDocs.size();
    long start = System.nanoTime();
    List<Put> puts = new ArrayList<Put>();
    for (final Map.Entry<String, Object> entry : this.termDocs.entrySet()) {
      Put put = new Put(Bytes.toBytes(entry.getKey()));
      byte[] docSet = null;
      if (entry.getValue() instanceof OpenBitSet) {
        docSet = Bytes.add( Bytes.toBytes('O')  ,HBaseneUtil.toBytes((OpenBitSet) entry.getValue()));
        //TODO: Scope for optimization
      } else if (entry.getValue() instanceof List) {
        List<Integer> list = (List<Integer>) entry.getValue();
        byte[] out = new byte[( list.size() + 1 ) * Bytes.SIZEOF_INT];
        for (int i = 0; i < list.size(); ++i) { 
          Bytes.putInt(out, (i + 1) * Bytes.SIZEOF_INT, list.get(i));
        }
        Bytes.putInt(out, 0, list.size());        
        docSet = Bytes.add( Bytes.toBytes('A'), out);
      }
      put.add(FAMILY_TERMVECTOR, Bytes.toBytes(this.docBase), docSet);
      put.setWriteToWAL(false);
      puts.add(put);
      if (puts.size() == 50000) {
        this.termVectorTable.put(puts);
        this.termVectorTable.flushCommits();
        puts.clear();
      }
    }
    this.termVectorTable.put(puts);
    this.termVectorTable.flushCommits();
    LOG.info("HBaseIndexStore#Flushed " + sz + " terms of "
        + this.termVectorTable + " in " + (double) (System.nanoTime() - start)
        / (double) 1000000 + " m.secs ");
    doIncrementSegmentId();
  }

  void doIncrementSegmentId() throws IOException {
    HTable table = this.tablePool.getTable(this.indexName);
    try {
      segmentId = table.incrementColumnValue(ROW_SEGMENT_ID, FAMILY_SEQUENCE,
          QUALIFIER_SEGMENT, 1, true);
    } finally {
      this.tablePool.putTable(table);
    }
  }

  @Override
  public void storeField(long docId, String fieldName, byte[] value)
      throws IOException {
    Put put = new Put(Bytes.toBytes(docId));
    put.add(FAMILY_FIELDS, Bytes.toBytes(fieldName), value);
    put.setWriteToWAL(false);// Do not write to val
    HTable table = this.tablePool.getTable(this.indexName);
    try {
      table.put(put);
    } finally {
      this.tablePool.putTable(table);
    }
  }

  @Override
  public long docId(final byte[] primaryKey) throws IOException {
    HTable table = this.tablePool.getTable(this.indexName);
    long newId = -1;
    try {
      // FIXIT: What if, primaryKey already exists in the table.
      // Atomic RPC to HBase region server

      newId = table.incrementColumnValue(ROW_SEQUENCE_ID, FAMILY_SEQUENCE,
          QUALIFIER_SEQUENCE, 1, false); // Do not worry about the WAL, at this
      // point of insertion.
      if (newId >= Integer.MAX_VALUE) {
        throw new IllegalStateException("API Limitation reached. ");
      }
      insertDocToInt(table, primaryKey, Bytes.toBytes(newId));
    } finally {
      this.tablePool.putTable(table);
    }
    return newId;
  }

  public byte[] getCellValue(final byte[] row, final byte[] columnFamily,
      final byte[] columnQualifier) throws IOException {
    HTable table = this.tablePool.getTable(this.indexName);
    try {
      Scan scan = new Scan(row);
      scan.addColumn(columnFamily, columnQualifier);
      ResultScanner scanner = table.getScanner(scan);
      try {
        Result result = scanner.next();
        return result.getValue(columnFamily, columnQualifier);
      } finally {
        scanner.close();
      }
    } finally {
      this.tablePool.putTable(table);
    }

  }

  void insertDocToInt(final HTable table, final byte[] primaryKey,
      final byte[] docId) throws IOException {
    Put put = new Put(primaryKey);
    put.add(FAMILY_DOC_TO_INT, QUALIFIER_INT, docId);
    put.setWriteToWAL(false);
    table.put(put);

  }

  /**
   * Drop the given Lucene index table.
   * 
   * @param tableName
   * @param configuration
   * @throws IOException
   */
  public static void dropLuceneIndexTable(final String tableName,
      final HBaseConfiguration configuration) throws IOException {
    HBaseAdmin admin = new HBaseAdmin(configuration);
    doDropTable(admin, tableName);
  }

  static void doDropTable(final HBaseAdmin admin, final String tableName)
      throws IOException {
    // TODO: The set of operations below are not atomic at all / Currently such
    // guarantee is not provided by HBase. Need to modify HBase RPC/ submit a
    // patch to incorporate the same.
    if (admin.tableExists(tableName)) {
      if (admin.isTableAvailable(tableName)) {
        admin.disableTable(tableName);
      }
      admin.deleteTable(tableName);
    }
  }

  /**
   * Create a table to store lucene indices, with the given name and the
   * configuration.
   * 
   * @param tableName
   *          Name of the table to hold lucene indices.
   * @param configuration
   *          Configuration to hold HBase schema.
   * @param forceRecreate
   *          Drop any old table if it exists by the same name.
   * @return a valid HTable reference to the table of the name, if created
   *         successfully. <br>
   *         null, if table was not created successfully.
   * @throws IOException
   *           in case of any error with regard to the same.
   */
  public static HTable createLuceneIndexTable(final String tableName,
      final HBaseConfiguration configuration, boolean forceRecreate)
      throws IOException {
    HBaseAdmin admin = new HBaseAdmin(configuration);

    if (admin.tableExists(tableName)) {
      if (!forceRecreate) {
        throw new IllegalArgumentException(
            "Table already exists by the index name " + tableName);
      } else {
        doDropTable(admin, tableName);
      }
    }

    HTableDescriptor tableDescriptor = new HTableDescriptor(Bytes
        .toBytes(tableName));
    tableDescriptor.addFamily(createUniversionLZO(admin, FAMILY_FIELDS));
    tableDescriptor.addFamily(createUniversionLZO(admin, FAMILY_TERMVECTOR));
    tableDescriptor.addFamily(createUniversionLZO(admin, FAMILY_TERMPOSITIONS));
    tableDescriptor.addFamily(createUniversionLZO(admin, FAMILY_DOC_TO_INT));
    tableDescriptor.addFamily(createUniversionLZO(admin, FAMILY_SEQUENCE));
    tableDescriptor.addFamily(createUniversionLZO(admin, FAMILY_PAYLOADS));

    admin.createTable(tableDescriptor);
    HTableDescriptor descriptor = admin.getTableDescriptor(Bytes
        .toBytes(tableName));

    if (descriptor != null) {
      HTable table = new HTable(configuration, tableName);

      Put put = new Put(ROW_SEQUENCE_ID);
      put.add(FAMILY_SEQUENCE, QUALIFIER_SEQUENCE, Bytes.toBytes(-1L));
      table.put(put);

      Put put2 = new Put(ROW_SEGMENT_ID);
      put2.add(FAMILY_SEQUENCE, QUALIFIER_SEGMENT, Bytes.toBytes(-1L));
      table.put(put2);

      table.flushCommits();

      return table;
    } else {
      return null;
    }
  }

  static HColumnDescriptor createUniversionLZO(final HBaseAdmin admin,
      final byte[] columnFamilyName) {
    HColumnDescriptor desc = new HColumnDescriptor(columnFamilyName);
    // TODO: Is there anyway to check the algorithms supported by HBase in the
    // admin interface ?
    // if (admin.isSupported(Algorithm.LZO)) {
    // desc.setCompressionType(Algorithm.LZO);
    // }
    desc.setMaxVersions(1);
    return desc;
  }
}
