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
package org.hbasene.index.search;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.Weight;
import org.hbasene.index.HBaseIndexReader;
import org.hbasene.index.HBaseneConstants;


/**
 * IndexSearcher
 */
public class HBaseIndexSearcher extends IndexSearcher implements
    HBaseneConstants {

  @SuppressWarnings("unused")
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
    return (fields.length == 1 && fields[0] == SortField.FIELD_SCORE) ? super
        .search(weight, filter, nDocs, sort, fillFields) : doSearch(weight,
        filter, nDocs, sort, fillFields);

  }

  TopFieldDocs doSearch(final Weight weight, Filter filter, int nDocs,
      Sort sort, boolean fillFields) throws IOException {
    HBaseTopFieldCollector topFieldCollector = new HBaseTopFieldCollector(
        this.tablePool, this.indexName, nDocs, sort);
    search(weight, filter, topFieldCollector);
    return (TopFieldDocs) topFieldCollector.topDocs();
  }

}
