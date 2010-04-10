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
package org.apache.nosql.lucene.index.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermPositions;

/**
 * Term Docs implementation for HBase.
 */
public class HBaseTermDocs implements TermPositions {

  private final HTable table;

  /**
   * List of documents corresponding to the term docs under consideration.
   */
  private List<byte[]> documents;
  // TODO:WeakRef this and load on demand, if taken away, to save memory.

  /**
   * Current index into the documents array.
   */
  private int currentIndex;

  /**
   * Current row (field/text) of the term under consideration.
   */
  private byte[] currentRow;

  private int[] currentTermPositions;

  private int currentTermPositionIndex;

  private Comparator<byte[]> INT_COMPARATOR = new Comparator<byte[]>() {

    @Override
    public int compare(byte[] o1, byte[] o2) {
      int lhs = Bytes.toInt(o1);
      int rhs = Bytes.toInt(o2);
      if (lhs < rhs)
        return -1;
      else if (lhs > rhs)
        return 1;
      else
        return 0;

    }

  };

  public HBaseTermDocs(final Configuration conf, final String indexName)
      throws IOException {
    this.table = new HTable(conf, indexName);
  }

  @Override
  public void close() throws IOException {
    documents.clear();
    currentIndex = 0;
  }

  @Override
  public int doc() {
    return Bytes.toInt(this.documents.get(this.currentIndex));
  }

  @Override
  public int freq() {
    return this.currentTermPositions.length;
  }

  @Override
  public boolean next() throws IOException {
    if (currentIndex < this.documents.size()) {
      this.currentIndex++;
      resetInternalData();
      return true;
    } else {
      return false;
    }
  }

  private void resetInternalData() throws IOException {
    Get get = new Get(this.currentRow);
    get.addColumn(HBaseIndexTransactionLog.FAMILY_TERM_VECTOR, this.documents
        .get(this.currentIndex));
    Result result = table.get(get);
    byte[] tfArray = result.getValue(
        HBaseIndexTransactionLog.FAMILY_TERM_VECTOR, this.documents
            .get(this.currentIndex));
    String tf = Bytes.toString(tfArray);
    currentTermPositionIndex = 0;
    if (tf == null) {
      currentTermPositions = new int[0];
    } else {
      String[] tfs = tf.split(",");
      this.currentTermPositions = new int[tfs.length];
      for (int i = 0; i < tfs.length; ++i) {
        this.currentTermPositions[i] = Integer.valueOf(tfs[i]);
      }
    }

  }

  @Override
  public int read(int[] docs, int[] freqs) throws IOException {
    int count = 0;
    for (int i = 0; i < docs.length; ++i) {
      if (next()) {
        docs[i] = this.doc();
        freqs[i] = this.freq();
        ++count;
      } else {
        break;
      }
    }
    return count;
  }

  @Override
  public void seek(Term term) throws IOException {
    final String rowKey = term.field() + "/" + term.text();
    this.currentRow = Bytes.toBytes(rowKey);
    Result result = this.getRowWithTermVectors();
    NavigableMap<byte[], byte[]> map = result
        .getFamilyMap(HBaseIndexTransactionLog.FAMILY_TERM_VECTOR);
    this.documents = new ArrayList<byte[]>(map.keySet());
    Collections.sort(documents, INT_COMPARATOR);
    this.currentIndex = 0;
  }

  Result getRowWithTermVectors() throws IOException {
    Get get = new Get(this.currentRow);
    get.addFamily(HBaseIndexTransactionLog.FAMILY_TERM_VECTOR);
    return this.table.get(get);
  }

  @Override
  public void seek(TermEnum termEnum) throws IOException {
    seek(termEnum.term());
  }

  @Override
  public boolean skipTo(int target) throws IOException {
    // TODO: Should the starting Index of the loop be 0 or currentIndex ?
    for (int i = 0; i < this.documents.size(); ++i) {
      if (Bytes.toInt(this.documents.get(i)) >= target) {
        currentIndex = i;
        return true;
      }
    }
    return false;
  }

  @Override
  public byte[] getPayload(byte[] data, int offset) throws IOException {
    return null;
  }

  @Override
  public int getPayloadLength() {
    return 0;
  }

  @Override
  public boolean isPayloadAvailable() {
    return false;
  }

  @Override
  public int nextPosition() throws IOException {
    return this.currentTermPositions[this.currentTermPositionIndex++];
  }
}
