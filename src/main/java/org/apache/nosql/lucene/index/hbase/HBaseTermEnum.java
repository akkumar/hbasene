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
package org.apache.nosql.lucene.index.hbase;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableRecordReaderImpl;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;

/**
 * Implementation of Term Enumerator. <br />
 * 
 * 
 * @author akkumar
 * 
 */
public class HBaseTermEnum extends TermEnum {

  /**
   * IDF Family
   */
  static final byte[] IDF_FAMILY = Bytes.toBytes("idf-family");

  /**
   * Look-Ahead buffer for the scan. Given that some common terms (across the
   * entire corpus) - the idf can be large, 5 is a reasonable number to begin
   * with.
   * 
   */
  static final int MAX_LOOK_AHEAD_BUFFER = 5;

  /**
   * Field/Term separator
   */
  static final int SEPARATOR = '/';

  /**
   * Default scan associated with the term enum.
   */
  final HTable table;

  /**
   * Scanner
   */
  final ResultScanner scanner;

  /**
   * Current Row
   */
  Result currentRow;

  public HBaseTermEnum(final HTable table,
      final TableRecordReaderImpl recordReader) throws IOException {
    Pair<byte[][], byte[][]> startEndKeys = table.getStartEndKeys();
    this.table = table;
    Scan scan = this.createScan();
    scan.setStartRow(startEndKeys.getFirst()[0]);
    this.scanner = this.table.getScanner(scan);
  }

  @Override
  public void close() throws IOException {
    this.scanner.close();
  }

  @Override
  public int docFreq() {
    NavigableMap<byte[], byte[]> map = this.currentRow.getNoVersionMap().get(
        IDF_FAMILY);
    return map.values().size();
  }

  @Override
  public boolean next() throws IOException {
    this.currentRow = this.scanner.next();
    return this.currentRow != null;
  }

  @Override
  public Term term() {
    ImmutableBytesWritable writable = this.currentRow.getBytes();
    String backingRow = Bytes.toString(writable.get(), writable.getOffset(),
        writable.getLength() - 1 - writable.getOffset());
    int index = backingRow.indexOf(SEPARATOR);
    if (index != -1) {
      String fieldName = backingRow.substring(writable.getOffset(), index);
      String termName = backingRow.substring(index + 1);
      return new Term(fieldName, termName);
    } else {
      return null;
    }
  }

  Scan createScan() {
    Scan scan = new Scan();
    scan.addFamily(IDF_FAMILY);
    scan.setBatch(MAX_LOOK_AHEAD_BUFFER);
    return scan;
  }
}
