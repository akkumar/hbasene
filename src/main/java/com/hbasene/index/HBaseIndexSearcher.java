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
package com.hbasene.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.Weight;

/**
 * IndexSearcher
 */
public class HBaseIndexSearcher extends IndexSearcher implements
    HBaseneConstants {

  private static final Log LOG = LogFactory.getLog(HBaseIndexSearcher.class);

  private final HTablePool tablePool;

  private final String indexName;

  public HBaseIndexSearcher(HBaseIndexReader indexReader)
      throws CorruptIndexException, IOException {
    super(indexReader);
    this.tablePool = indexReader.getTablePool();
    this.indexName = indexReader.getIndexName();
  }

  @Override
  public TopFieldDocs search(Weight weight, Filter filter, final int nDocs,
      Sort sort, boolean fillFields) throws IOException {
    SortField[] fields = sort.getSort();
    if (fields.length > 1) {
      throw new IllegalArgumentException(
          "Multiple Sort fields not supported at the moment");
    }
    if (fields[0] == SortField.FIELD_SCORE) {
      return super.search(weight, filter, nDocs, sort, fillFields);
    } else {
      // TODO: Sort based on the custom sort field
      return super.search(weight, filter, nDocs, sort, fillFields);
    }
  }

  public ScoreDoc[] sort(final ScoreDoc[] scoreDocs, final String sortField)
      throws IOException {
    HTableInterface table = this.tablePool.getTable(this.indexName);
    List<ScoreDoc> input = new LinkedList<ScoreDoc>();
    for (ScoreDoc inputDoc : scoreDocs) {
      input.add(inputDoc);
    }
    ScoreDoc[] output = new ScoreDoc[scoreDocs.length];
    int outputIndex = 0;
    final String sortFieldPrefix = sortField + "/"; // separator
    try {
      byte[] row = Bytes.toBytes(sortFieldPrefix);
      Result priorToFirstTerm = table.getRowOrBefore(row, FAMILY_TERMVECTOR);
      ResultScanner scanner = table.getScanner(this
          .createScan((priorToFirstTerm != null) ? priorToFirstTerm.getRow()
              : null));
      try {
        Result result = scanner.next();
        while (result != null) {
          String currentRow = Bytes.toString(result.getRow());
          if (currentRow.startsWith(sortFieldPrefix)) {
            NavigableMap<byte[], byte[]> columnQualifiers = result
                .getFamilyMap(FAMILY_TERMVECTOR);
            List<Long> docIds = new ArrayList<Long>();
            for (Map.Entry<byte[], byte[]> columnQualifier : columnQualifiers
                .entrySet()) {
              docIds.add(HBaseTermPositions.BYTES_TO_DOCID
                  .apply(columnQualifier.getKey()));
            }
            LOG.info(currentRow + " --> " + docIds);
            Iterator<ScoreDoc> it = input.iterator();
            while (it.hasNext()) {
              ScoreDoc next = it.next();
              if (columnQualifiers.containsKey(Bytes.toBytes((long) next.doc))) {
                output[outputIndex++] = next;
                it.remove();
              }
            }
          }
          result = scanner.next();
        }
        if (outputIndex != scoreDocs.length) {
          LOG.warn("Output Index " + outputIndex
              + " not the same as the input length " + scoreDocs.length);
        }
      } finally {
        scanner.close();
      }
      return output;
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
}
