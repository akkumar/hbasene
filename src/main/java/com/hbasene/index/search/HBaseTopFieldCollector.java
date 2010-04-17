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
package com.hbasene.index.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;

import com.hbasene.index.HBaseTermPositions;
import com.hbasene.index.HBaseneConstants;

/**
 * Sorting implementation
 */
public final class HBaseTopFieldCollector extends Collector implements
    HBaseneConstants {

  private static final Log LOG = LogFactory
      .getLog(HBaseTopFieldCollector.class);

  private static final int DOCS_THRESHOLD = 1000;

  private final HTablePool tablePool;

  private final String indexName;

  private final int nDocs;

  private final LinkedList<SortFieldDoc> docs = new LinkedList<SortFieldDoc>();

  private final PriorityQueue<SortFieldDoc> pq;

  private final SortField[] fields;

  private Scorer scorer;

  private int pendingDocs;

  private int totalHits;

  private static final Comparator<SortFieldDoc> ASCENDING_COMPARATOR = new SortFieldDocComparatorAsc();

  private static final Comparator<SortFieldDoc> DESCENDING_COMPARATOR = new SortFieldDocComparatorDesc();

  public HBaseTopFieldCollector(final HTablePool tablePool,
      final String indexName, final int nDocs, final Sort sort) {
    this.tablePool = tablePool;
    this.indexName = indexName;
    this.nDocs = nDocs;
    this.fields = sort.getSort();
    this.pendingDocs = 0;
    this.totalHits = 0;
    this.pq = new PriorityQueue<SortFieldDoc>(nDocs, this.fields[0]
        .getReverse() ? DESCENDING_COMPARATOR : ASCENDING_COMPARATOR);

    if (fields.length > 1) {
      throw new IllegalArgumentException("Multiple fields not supported yet ");
    }
  }

  @Override
  public void collect(int doc) throws IOException {
    float currentScore = 1.0f;
    if (this.scorer != null) {
      currentScore = this.scorer.score();
    }
    docs.add(new SortFieldDoc(doc, currentScore, 1));// only 1 sort field under
    // consideration at the
    // moment.
    ++pendingDocs;
    if (this.pendingDocs == DOCS_THRESHOLD) {
      this.appendToPQ();
      this.pendingDocs = 0;
      docs.clear();
    }
    ++this.totalHits;
  }

  @Override
  public void setNextReader(IndexReader reader, int docBase) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
    this.scorer = scorer;
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return true;
  }

  TopFieldDocs topDocs() throws IOException {
    if (this.pendingDocs > 0) {
      this.appendToPQ();
      this.pendingDocs = 0;
      docs.clear();
    }
    int arraySize = (this.totalHits >= this.nDocs) ? this.nDocs
        : this.totalHits;
    ScoreDoc[] scoreDocs = new ScoreDoc[arraySize];
    for (int i = 0; i < this.nDocs; ++i) {
      scoreDocs[i] = this.pq.poll();
    }
    return new TopFieldDocs(this.totalHits, scoreDocs, this.fields, 0.0f);
  }

  public void appendToPQ() throws IOException {
    this.doAppendToPQ(this.docs, this.pq, this.fields[0].getField());
  }

  private void doAppendToPQ(final LinkedList<SortFieldDoc> docs,
      final PriorityQueue<SortFieldDoc> outputPq, final String sortField)
      throws IOException {
    HTableInterface table = this.tablePool.getTable(this.indexName);
    final String sortFieldPrefix = sortField + "/"; // separator
    try {
      byte[] row = Bytes.toBytes(sortFieldPrefix);
      Result priorToFirstTerm = table.getRowOrBefore(row, FAMILY_TERMVECTOR);
      ResultScanner scanner = table.getScanner(this
          .createScan((priorToFirstTerm != null) ? priorToFirstTerm.getRow()
              : null));
      try {
        int index = 0;
        Result result = scanner.next();
        while (result != null) {
          String currentRow = Bytes.toString(result.getRow());
          if (currentRow.startsWith(sortFieldPrefix)) {
            ++index;
            NavigableMap<byte[], byte[]> columnQualifiers = result
                .getFamilyMap(FAMILY_TERMVECTOR);
            List<Long> docIds = new ArrayList<Long>();
            for (Map.Entry<byte[], byte[]> columnQualifier : columnQualifiers
                .entrySet()) {
              docIds.add(HBaseTermPositions.BYTES_TO_DOCID
                  .apply(columnQualifier.getKey()));
            }
            LOG.info(currentRow + " --> " + docIds);
            Iterator<SortFieldDoc> it = docs.iterator();
            while (it.hasNext()) {
              SortFieldDoc next = it.next();
              if (columnQualifiers.containsKey(Bytes.toBytes((long) next.doc))) {
                next.indices[0] = index;
                outputPq.add(next);
                it.remove();
              }
            }
            if (docs.isEmpty()) {
              break;
            }
          }
          result = scanner.next();
        }
      } finally {
        scanner.close();
      }
    } finally {
      this.tablePool.putTable(table);
    }
  }

  private Scan createScan(final byte[] prefix) {
    Scan scan = new Scan();
    if (prefix != null) {
      scan.setStartRow(prefix);
    }
    scan.addFamily(FAMILY_TERMVECTOR);
    scan.setCaching(20);
    return scan;
  }

  private static class SortFieldDoc extends ScoreDoc {

    private int[] indices;

    public SortFieldDoc(int doc, float score, int totalLength) {
      super(doc, score);
      this.indices = new int[totalLength];
      for (int i = 0; i < totalLength; ++i) {
        indices[i] = -1;
      }
    }
  }

  private static class SortFieldDocComparatorAsc implements
      Comparator<SortFieldDoc> {

    @Override
    public int compare(SortFieldDoc lhs, SortFieldDoc rhs) {

      if (lhs.indices.length > rhs.indices.length) {
        return 1;
      } else if (lhs.indices.length < rhs.indices.length) {
        return -1;
      } else {
        for (int i = 0; i < lhs.indices.length; ++i) {
          if (lhs.indices[i] > rhs.indices[i]) {
            return 1;
          } else if (lhs.indices[i] < rhs.indices[i]) {
            return -1;
          }
        }
        // Score is implied as the last one.
        return Float.compare(lhs.score, rhs.score);
      }
    }

  }

  private static class SortFieldDocComparatorDesc implements
      Comparator<SortFieldDoc> {

    @Override
    public int compare(SortFieldDoc lhs, SortFieldDoc rhs) {

      if (lhs.indices.length > rhs.indices.length) {
        return 1;
      } else if (lhs.indices.length < rhs.indices.length) {
        return -1;
      } else {
        for (int i = 0; i < lhs.indices.length; ++i) {
          if (lhs.indices[i] < rhs.indices[i]) {
            return 1;
          } else if (lhs.indices[i] > rhs.indices[i]) {
            return -1;
          }
        }
        // Score is implied as the last one.
        return Float.compare(lhs.score, rhs.score);
      }
    }

  }

}
