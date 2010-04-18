/**
 * Copyright 2010 Karthik Kumar
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

import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.lucene.index.TermFreqVector;

/**
 * HBase implementation of the Term Frequency Vector
 */
public class HBaseTermFreqVector implements TermFreqVector {

  private HTablePool tablePool;

  private String indexName;

  public HBaseTermFreqVector(final HBaseIndexReader indexReader) {
    this.tablePool = indexReader.getTablePool();
    this.indexName = indexReader.getIndexName();
  }

  @Override
  public String getField() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int[] getTermFrequencies() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String[] getTerms() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int indexOf(String term) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int[] indexesOf(String[] terms, int start, int len) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int size() {
    // TODO Auto-generated method stub
    return 0;
  }

}
