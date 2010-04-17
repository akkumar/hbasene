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
import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.PriorityQueue;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.search.HitCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.hbasene.index.HBaseneConstants;

/**
 * Sorting implementation
 */
public final class HBaseTopFieldCollector extends HitCollector implements
    HBaseneConstants {

  private static final Log LOG = LogFactory
      .getLog(HBaseTopFieldCollector.class);

  private static final int DOCS_THRESHOLD = 1000;

  private final HTablePool tablePool;

  private final String indexName;

  private final int nDocs;

  private final NavigableMap<byte[], SortFieldDoc> docMap = new TreeMap<byte[], SortFieldDoc>(
      new BytesAsLongComparator());

  private final PriorityQueue<SortFieldDoc> pq;

  private final SortField[] fields;

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

  public void collect(int doc, float score) {
    byte[] key = Bytes.toBytes((long) doc);
    docMap.put(key, new SortFieldDoc(doc, score, 1));
    // TODO: only 1 sort field under consideration now
    ++pendingDocs;
    if (this.pendingDocs == DOCS_THRESHOLD) {
      this.appendToPQ();
      this.pendingDocs = 0;
    }
    ++this.totalHits;
  }

  TopFieldDocs topDocs() throws IOException {
    if (this.pendingDocs > 0) {
      this.appendToPQ();
      this.pendingDocs = 0;
    }
    int arraySize = (this.totalHits >= this.nDocs) ? this.nDocs
        : this.totalHits;
    ScoreDoc[] scoreDocs = new ScoreDoc[arraySize];
    for (int i = 0; i < this.nDocs; ++i) {
      scoreDocs[i] = this.pq.poll();
    }
    return new TopFieldDocs(this.totalHits, scoreDocs, this.fields, 0.0f);
  }


  public void appendToPQ()  {
    try {
      this.doAppendToPQ(this.docMap, this.pq, this.fields[0].getField(), 0);
    } catch (Exception ex) {
      LOG.warn("Error occured while pushing", ex);
    }
  }

  private void doAppendToPQ(final Map<byte[], SortFieldDoc> docMap,
      final PriorityQueue<SortFieldDoc> outputPq, final String sortField,
      final int sortIndex) throws IOException {
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
            SetView<byte[]> intersectionSet = Sets.intersection(
                columnQualifiers.keySet(), docMap.keySet());
            for (final byte[] commonDocId : intersectionSet) {
              SortFieldDoc next = docMap.get(commonDocId);
              next.indices[sortIndex] = index;
              outputPq.add(next);
            }
            //Method works best if the ratio between the unique number of elements 
            // in the field to be sorted is small compared to the total 
            // number of documents in the list
            docMap.keySet().removeAll(intersectionSet);
            LOG.info("Docs Size after  " + currentRow + " is " + docMap.size());
            if (docMap.isEmpty()) {
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

  private static class BytesAsLongComparator implements Comparator<byte[]> {

    @Override
    public int compare(byte[] o1, byte[] o2) {
      long lhs = Bytes.toLong(o1);
      long rhs = Bytes.toLong(o2);
      if (lhs > rhs) {
        return -1;
      } else if (lhs < rhs) {
        return 1;
      } else {
        return 0;
      }
    }

  }

}
