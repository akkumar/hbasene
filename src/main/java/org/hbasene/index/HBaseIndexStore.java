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
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.lucene.HBaseneUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.util.OpenBitSet;

/**
 * An index formed on-top of HBase.
 * <p>
 * Important:
 * </p>
 * This class is not thread-safe.
 * 
 * To create a HBase Table, specific to the index schema, refer to
 * {@link #createLuceneIndexTable(String, HBaseConfiguration, boolean)} .
 */
public class HBaseIndexStore extends AbstractIndexStore implements
    HBaseneConstants {

  private static final Log LOG = LogFactory.getLog(HBaseIndexStore.class);

  private final Map<String, Object> termVector = new HashMap<String, Object>();

  private long segmentId = 0;

  private int documentId = -1;

  private final int maxCommitDocs = 1000;

  /**
   * For maximum throughput, use a single table, since the .META. of the term
   * vector is cached in the table as we continue to add more information about
   * the terms to the table.
   */
  private final HTable table;

  /**
   * Encoder of termPositions
   */
  // TODO: Better encoding rather than the integer form is needed.
  // Use OpenBitSet preferably again, for term frequencies
  private final AbstractTermPositionsEncoder termPositionEncoder = new AlphaTermPositionsEncoder();

  public HBaseIndexStore(final HTablePool tablePool,
      final HBaseConfiguration configuration, final String indexName)
      throws IOException {
    this.table = tablePool.getTable(indexName);

    this.doIncrementSegmentId();
  }

  @Override
  public synchronized void close() throws IOException {
    commit();
  }

  @Override
  public synchronized void commit() throws IOException {
    this.doCommit();
    // this.doCommit();
    // TODO: Close all tables in the tablepool
    // HTable table = this.tablePool.getTable(this.indexName);
    // try {
    // table.close();
    // } finally {
    // this.tablePool.putTable(table);
    // }
  }

  /**
   * Index a given document.
   * 
   * @param key
   * @param documentIndexContext
   * @return SegmentInfo that contains a segment id and a document id.
   * @throws IOException
   */
  public synchronized SegmentInfo indexDocument(final String key,
      final DocumentIndexContext documentIndexContext) throws IOException {
    ++this.documentId;
    final byte[] currentRow = this.getCurrentRow();
    this.doAddTermVector(documentId, documentIndexContext.termPositionVectors
        .keySet());
    // TODO: Store the term frequency as well.
    // this.doAddTermFrequency(documentId,
    // documentIndexContext.termPositionVectors);
    this.doStoreFields(currentRow, documentIndexContext.storeFields);
    this.doStoreReverseMapping(key, currentRow);
    SegmentInfo segmentInfo = new SegmentInfo(this.segmentId, this.documentId);
    if (this.documentId == this.maxCommitDocs) {
      doCommit();
    }
    return segmentInfo;

  }

  void doStoreReverseMapping(final String key, final byte[] currentRow) {
    Put put = new Put(Bytes.toBytes(key));
    put.add(FAMILY_DOC_TO_INT, QUALIFIER_INT, currentRow);
    put.setWriteToWAL(true);
    this.table.getWriteBuffer().add(put);
  }

  void doAddTermFrequency(final int docId,
      final Map<String, List<Integer>> termFrequencies) throws IOException {
    List<Put> puts = new ArrayList<Put>();
    for (final Map.Entry<String, List<Integer>> entry : termFrequencies
        .entrySet()) {
      Put put = new Put(Bytes.toBytes(HBaseneConstants.TERM_FREQ_PREFIX + "/"
          + entry.getKey()));
      put.add(HBaseneConstants.FAMILY_TERMFREQUENCIES, Bytes.toBytes(docId),
          Bytes.toBytes(termFrequencies.size()));
      puts.add(put);
    }
    this.table.getWriteBuffer().addAll(puts);
  }

  void doAddTermVector(final int docId, final Set<String> fieldTerms)
      throws IOException {
    for (final String fieldTerm : fieldTerms) {
      Object bitset = this.termVector.get(fieldTerm);
      if (bitset == null) {
        bitset = new ArrayList<Integer>();// Allocate at least 1 long
        this.termVector.put(fieldTerm, bitset);
      }
      if (bitset instanceof List) {
        List<Integer> impl = (List<Integer>) bitset;
        impl.add(docId);
        if (impl.size() > 10) { 
          
        }
      } 
      if (bitset instanceof OpenBitSet) {
        ((OpenBitSet) bitset).set(docId);
      }
    }
  }

  private void doCommit() throws IOException {
    final int sz = this.termVector.size();
    final long start = System.nanoTime();
    for (final Map.Entry<String, Object> entry : this.termVector.entrySet()) {
      final String key = entry.getKey();
      Put put = new Put(Bytes.toBytes(key));
      byte[] docSet = null;
      docSet = Bytes.add(Bytes.toBytes('O'), HBaseneUtil
          .toBytes((OpenBitSet) entry.getValue()));
      put.add(HBaseneConstants.FAMILY_TERMVECTOR, Bytes.toBytes("s"
          + this.segmentId), docSet);
      put.setWriteToWAL(true);
      this.table.getWriteBuffer().add(put);
    }
    this.termVector.clear();
    this.table.flushCommits();
    LOG.info("HBaseIndexStore#Flushed " + sz + " terms of " + table + " in "
        + (double) (System.nanoTime() - start) / (double) 1000000 + " m.secs ");
    this.documentId = 0;
    doIncrementSegmentId();
  }

  long doIncrementSegmentId() throws IOException {
    return this.table.incrementColumnValue(ROW_SEGMENT_ID, FAMILY_SEQUENCE,
        QUALIFIER_SEGMENT, 1, true);
  }

  void doStoreFields(final byte[] currentRow,
      final Map<String, byte[]> fieldsToStore) throws IOException {
    for (final Map.Entry<String, byte[]> entry : fieldsToStore.entrySet()) {
      Put put = new Put(currentRow);
      put.add(FAMILY_FIELDS, Bytes.toBytes(entry.getKey()), entry.getValue());
      put.setWriteToWAL(true);// Do not write to val
      this.table.getWriteBuffer().add(put);
    }
  }

  byte[] getCurrentRow() {
    return Bytes.toBytes("s" + this.segmentId + "/" + this.documentId);
  }

  void insertUserIdToSegmentId(final HTable table, final byte[] primaryKey,
      final byte[] docId) throws IOException {
    Put put = new Put(primaryKey);
    put.add(FAMILY_DOC_TO_INT, QUALIFIER_INT, docId);
    put.setWriteToWAL(true);
    table.put(put);

  }

  // TABLE MANIPULATION ROUTINES .

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
    tableDescriptor
        .addFamily(createUniversionLZO(admin, FAMILY_TERMFREQUENCIES));
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
    desc.setCompressionType(Algorithm.GZ);
    // TODO: Is there anyway to check the algorithms supported by HBase in the
    // admin interface ?
    // if (admin.isSupported(Algorithm.LZO)) {
    // desc.setCompressionType(Algorithm.LZO);
    // }
    desc.setMaxVersions(1);
    return desc;
  }
}
