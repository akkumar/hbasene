/**
 * Copyright 2010 Karthik Kumar
 *
 * Based off the original code by Lucandra project, (C): Jake Luciani
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.index.TermVectorMapper;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.hadoop.hbase.client.HBaseneConstants;

/**
 * Index Reader specific to HBase
 * 
 */
public class HBaseIndexReader extends IndexReader {

  private static final Log LOG = LogFactory.getLog(HBaseIndexReader.class);

  /**
   * The indexName represents the tableName as well.
   */
  private final String indexName;

  /**
   * HTable is not thread-safe, but HTable caches the region locations within.
   * Rudimentary implementation of a table pool used.
   */
  private final HTablePool tablePool;

  /**
   * The default norm as per the given field.
   */
  static final byte DEFAULT_NORM = DefaultSimilarity.encodeNorm(1.0f);

  /**
   * Encoder of termPositions
   */
  private final AbstractTermPositionsEncoder termPositionEncoder = new AlphaTermPositionsEncoder();
  //TODO: Use HBaseIndexStore to do this 
  
  /**
   * Primary Key Field
   */
  private final byte[] primaryKeyField;
  
  
  /**
   * 
   * @param tablePool
   *    TablePool to be used by the index reader
   * @param indexName
   *    Name of the index to be read from.  
   */
  public HBaseIndexReader(final HTablePool tablePool, final String indexName, final String primaryKeyField) {
    this.tablePool = tablePool;
    this.indexName = indexName;
    this.primaryKeyField = Bytes.toBytes(primaryKeyField);
  }

  @Override
  protected void doClose() throws IOException {
    // TODO: HBASE-2435 in place.
    // this.tablePool.closeTablePool(this.indexName);
    // for (int i = 0; i < this.tablePoolSize; ++i) {
    // this.tablePool.getTable(this.indexName).close();
    // }
  }

  @Override
  protected void doCommit(Map<String, String> commitUserData)
      throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  protected void doDelete(int docNum) throws CorruptIndexException, IOException {
    // TODO Auto-generated method stub

  }

  @Override
  protected void doSetNorm(int doc, String field, byte value)
      throws CorruptIndexException, IOException {
    // TODO Auto-generated method stub

  }

  @Override
  protected void doUndeleteAll() throws CorruptIndexException, IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public int docFreq(Term t) throws IOException {
    // same as in TermEnum. Avoid duplication.
    final String rowKey = t.field() + "/" + t.text();
    Get get = new Get(Bytes.toBytes(rowKey));
    get.addFamily(HBaseneConstants.FAMILY_TERMVECTOR);
    HTable table = this.getTablePool().getTable(this.indexName);
    try {
      Result result = table.get(get);
      if (result == null) {
        return 0;
      }
      NavigableMap<byte[], byte[]> map = result
          .getFamilyMap(HBaseneConstants.FAMILY_TERMVECTOR);
      if (map == null) {
        return 0;
      }
      return map.size();
    } finally {
      this.getTablePool().putTable(table);
    }
  }

  @Override
  public Document document(int n, FieldSelector fieldSelector)
      throws CorruptIndexException, IOException {
    final long index = (long) n; // internally, all row keys are long.
    Document doc = null;
    HTable table = this.getTablePool().getTable(this.indexName);
    try {
      Get get = new Get(Bytes.toBytes(index));
      get.addColumn(HBaseneConstants.FAMILY_FIELDS,
          this.primaryKeyField);
      doc = new Document();

      Result result = table.get(get);
      byte[] docId = result.getValue(HBaseneConstants.FAMILY_FIELDS,
          this.primaryKeyField);
      // TODO: Get the document schema, for the given document.
      // Change the HBaseIndexWriter appropriately to enable easy
      // reconstruction.

      // For now, only the id is available for assertion back.
      doc.add(new Field("id", Bytes.toString(docId), Field.Store.YES,
          Field.Index.NO));

    } finally {
      this.getTablePool().putTable(table);
    }
    return doc;
  }

  @Override
  public Collection<String> getFieldNames(FieldOption fldOption) {
    return Arrays.asList(new String[] {});
  }

  @Override
  public TermFreqVector getTermFreqVector(int docNumber, String field)
      throws IOException {

    return null;
  }

  @Override
  public void getTermFreqVector(int docNumber, TermVectorMapper mapper)
      throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void getTermFreqVector(int docNumber, String field,
      TermVectorMapper mapper) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public TermFreqVector[] getTermFreqVectors(int docNumber) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean hasDeletions() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isDeleted(int n) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public int maxDoc() {
    return this.numDocs() + 1;
  }

  @Override
  public byte[] norms(String field) throws IOException {
    byte[] result = new byte[this.maxDoc()];
    Arrays.fill(result, DEFAULT_NORM);
    return result;
  }

  @Override
  public void norms(String field, byte[] bytes, int offset) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public int numDocs() {
    HTable table = this.getTablePool().getTable(this.indexName);
    int numDocs = 0;
    try {
      Get get = new Get(HBaseneConstants.ROW_SEQUENCE_ID);
      get.addColumn(HBaseneConstants.FAMILY_SEQUENCE,
          HBaseneConstants.QUALIFIER_SEQUENCE);
      Result result = table.get(get);
      numDocs = (int) Bytes.toLong(result
          .getValue(HBaseneConstants.FAMILY_SEQUENCE,
              HBaseneConstants.QUALIFIER_SEQUENCE));
    } catch (IOException e) {
      LOG.warn("Error in numDocs() ", e);
    } finally {
      this.getTablePool().putTable(table);
    }
    return numDocs;
  }

  @Override
  public TermDocs termDocs() throws IOException {
    return new HBaseTermPositions(this, this.termPositionEncoder);
  }

  @Override
  public TermPositions termPositions() throws IOException {
    return new HBaseTermPositions(this, this.termPositionEncoder);

  }

  @Override
  public TermEnum terms() throws IOException {
    return new HBaseTermEnum(this);
  }

  @Override
  public TermEnum terms(Term t) throws IOException {
    HBaseTermEnum termEnum = (HBaseTermEnum) terms();
    termEnum.skipTo(t);
    return termEnum;
  }

  /**
   * Create a reference to HTable to the index under consideration.
   * 
   * @return
   * @throws IOException
   */
  public HTablePool getTablePool() {
    return this.tablePool;
  }

  /**
   * Return the index Name of the given table.
   * 
   * @return
   */
  public String getIndexName() {
    return this.indexName;
  }
}
