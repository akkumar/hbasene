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
package org.apache.nosql.lucene.index.hbase;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.index.TermVectorMapper;

/**
 * Index Reader specific to HBase
 * 
 */
public class HBaseIndexReader extends IndexReader {

  private final Configuration conf;

  private final String indexName;

  /**
   * 
   * @param conf
   *          HBase Configuration needed for the client to establish the
   *          connection with the HBase Pool.
   * @param indexName
   *          Name of the index.
   */
  public HBaseIndexReader(final Configuration conf, final String indexName) {
    this.conf = conf;
    this.indexName = indexName;
  }

  @Override
  protected void doClose() throws IOException {
    // TODO Auto-generated method stub

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
    get.addFamily(HBaseIndexTransactionLog.FAMILY_TERMVECTOR);
    HTable table = this.getHTable();
    Result result = table.get(get);
    if (result == null) {
      return 0;
    }
    NavigableMap<byte[], byte[]> map = result
        .getFamilyMap(HBaseIndexTransactionLog.FAMILY_TERMVECTOR);
    if (map == null) {
      return 0;
    }
    return map.size();
  }

  @Override
  public Document document(int n, FieldSelector fieldSelector)
      throws CorruptIndexException, IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Collection<String> getFieldNames(FieldOption fldOption) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TermFreqVector getTermFreqVector(int docNumber, String field)
      throws IOException {
    // TODO Auto-generated method stub
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
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public byte[] norms(String field) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void norms(String field, byte[] bytes, int offset) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public int numDocs() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public TermDocs termDocs() throws IOException {
    return new HBaseTermPositions(this.conf, this.indexName);
  }

  @Override
  public TermPositions termPositions() throws IOException {
    return new HBaseTermPositions(this.conf, this.indexName);

  }

  @Override
  public TermEnum terms() throws IOException {
    return new HBaseTermEnum(this.conf, this.indexName);
  }

  @Override
  public TermEnum terms(Term t) throws IOException {
    HBaseTermEnum termEnum = new HBaseTermEnum(this.conf, this.indexName);
    termEnum.skipTo(t);
    return termEnum;
  }

  private HTable getHTable() throws IOException {
    return new HTable(this.conf, this.indexName);
  }
}
