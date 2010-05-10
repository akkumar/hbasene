/**
 * Copyright 2010 Karthik Kumar
 * 
 * Based off the original code by Lucandra project, (C): Jake Luciani
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
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.LockObtainFailedException;

import com.google.common.collect.Lists;

/**
 * Index Writer in the noSQL world.
 * 
 */
public class HBaseIndexWriter { // TODO: extends IndexWriter {

  private static final Log LOG = LogFactory.getLog(HBaseIndexWriter.class);
  
  /**
   * Abstraction of the index store
   */
  private final AbstractIndexStore indexStore;

  /**
   * Field representing the primary key in the given search index.
   */
  private final String primaryKeyField;


  /**
   * List of empty term vectors
   */
  static final List<Integer> EMPTY_TERM_POSITIONS = Arrays
      .asList(new Integer[] { 0 });

  /**
   * 
   * @param indexTransactionLog
   * @param primaryKeyField
   *          The primary key field of the lucene Document schema to be used.
   * @throws CorruptIndexException
   * @throws LockObtainFailedException
   * @throws IOException
   */
  public HBaseIndexWriter(final AbstractIndexStore indexTransactionLog,
      final String primaryKeyField) throws IOException {
    // super(d, a, create, deletionPolicy, mfl);
    // TODO: bring super ctor in when we inherit from IndexWriter.

    this.indexStore = indexTransactionLog;
    this.primaryKeyField = primaryKeyField;

    // Reset the transaction Log.
    // this.indexStore.init();
  }

  public void addDocument(Document doc, Analyzer analyzer)
      throws CorruptIndexException, IOException {
    String docId = doc.get(this.primaryKeyField);
    if (docId == null) {
      throw new IllegalArgumentException("Primary Key " + this.primaryKeyField
          + " not present in the document to be added ");
      // TODO: Special type of exception needed ?

    }
    int position = 0;
    Map<String, List<Integer>> termPositions = new HashMap<String, List<Integer>>();
    Map<String, byte[]> fieldsToStore = new HashMap<String, byte[]>();

    for (Fieldable field : doc.getFields()) {

      // Indexed field
      if (field.isIndexed() && field.isTokenized()) {
        TokenStream tokens = field.tokenStreamValue();

        if (tokens == null) {
          tokens = analyzer.tokenStream(field.name(), new StringReader(field
              .stringValue()));
        }
        tokens.addAttribute(TermAttribute.class);
        tokens.addAttribute(PositionIncrementAttribute.class);

        // collect term frequencies per doc
        if (position > 0) {
          position += analyzer.getPositionIncrementGap(field.name());
        }

        // Build the termPositions vector for all terms
        while (tokens.incrementToken()) {
          String term = createColumnName(field.name(), tokens.getAttribute(
              TermAttribute.class).term());

          List<Integer> pvec = termPositions.get(term);

          if (pvec == null) {
            pvec = Lists.newArrayList();
            termPositions.put(term, pvec);
          }

          position += (tokens.getAttribute(PositionIncrementAttribute.class)
              .getPositionIncrement() - 1);
          pvec.add(++position);

        }
        tokens.close();

      }

      // Untokenized fields go in without a termPosition
      if (field.isIndexed() && !field.isTokenized()) {
        String term = this.createColumnName(field.name(), field.stringValue());
        String key = term;
        termPositions.put(key, EMPTY_TERM_POSITIONS);

      }

      // Stores each field as a column under this doc key
      if (field.isStored()) {

        byte[] value = field.isBinary() ? field.getBinaryValue() : Bytes
            .toBytes(field.stringValue());

        // first byte flags if binary or not
        final byte[] prefix = Bytes.toBytes((field.isBinary() ? 'B' : 'T'));

        fieldsToStore.put(field.name(), Bytes.add(prefix, value));
      }
    }
    indexStore.indexDocument(docId, new DocumentIndexContext(termPositions,
        fieldsToStore));
    termPositions.clear();
    fieldsToStore.clear();
  }

  public void commit() throws IOException {
    this.indexStore.commit();
  }

  public void close() throws IOException {
    this.indexStore.close();
  }

  String createColumnName(final String fieldName, final String term) {
    return fieldName + "/" + term;
  }
}
