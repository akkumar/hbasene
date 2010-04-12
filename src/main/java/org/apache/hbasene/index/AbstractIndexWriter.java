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
package org.apache.hbasene.index;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.LockObtainFailedException;

/**
 * Index Writer in the noSQL world.
 * 
 */
public class AbstractIndexWriter { // TODO: extends IndexWriter {

  /**
   * Commit Log from the given index.
   */
  private final AbstractIndexTransactionLog indexTransactionLog;

  /**
   * Field representing the primary key in the given search index.
   */
  private final String primaryKeyField;

  /**
   * The default capacity of the number of terms in the given document.
   */
  private static final int DEFAULT_TERM_CAPACITY = 50;

  /**
   * List of empty term vectors
   */
  private static final List<Integer> EMPTY_TERM_VECTOR = Arrays
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
  public AbstractIndexWriter(
      final AbstractIndexTransactionLog indexTransactionLog,
      final String primaryKeyField) throws CorruptIndexException,
      LockObtainFailedException, IOException {
    // super(d, a, create, deletionPolicy, mfl);
    // TODO: bring super ctor in when we inherit from IndexWriter.

    this.indexTransactionLog = indexTransactionLog;
    this.primaryKeyField = primaryKeyField;

    // Reset the transaction Log.
    this.indexTransactionLog.init();
  }

  public void addDocument(Document doc, Analyzer analyzer)
      throws CorruptIndexException, IOException {
    String docId = doc.get(this.primaryKeyField);
    if (docId == null) {
      throw new IllegalArgumentException("Primary Key " + this.primaryKeyField
          + " not present in the document to be added ");
      // TODO: Special type of exception needed ?

    }

    long internalDocId = indexTransactionLog.assignDocId(Bytes.toBytes(docId));

    List<String> allIndexedTerms = new ArrayList<String>(DEFAULT_TERM_CAPACITY);

    int position = 0;

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
        Map<String, List<Integer>> termPositions = new HashMap<String, List<Integer>>();
        if (position > 0) {
          position += analyzer.getPositionIncrementGap(field.name());
        }

        // Build the termPositions vector for all terms
        while (tokens.incrementToken()) {
          String term = createColumnName(field.name(), tokens.getAttribute(
              TermAttribute.class).term());
          allIndexedTerms.add(term);

          List<Integer> pvec = termPositions.get(term);

          if (pvec == null) {
            pvec = new ArrayList<Integer>();
            termPositions.put(term, pvec);
          }

          position += (tokens.getAttribute(PositionIncrementAttribute.class)
              .getPositionIncrement() - 1);
          pvec.add(++position);

        }

        for (Map.Entry<String, List<Integer>> term : termPositions.entrySet()) {
          String key = term.getKey();
          indexTransactionLog.addTermVectors(key, Bytes.toBytes(internalDocId),
              term.getValue());
        }
      }

      // Untokenized fields go in without a termPosition
      if (field.isIndexed() && !field.isTokenized()) {
        String term = this.createColumnName(field.name(), field.stringValue());
        allIndexedTerms.add(term);

        String key = term;

        indexTransactionLog.addTermVectors(key, Bytes.toBytes(internalDocId),
            EMPTY_TERM_VECTOR);
      }

      // Stores each field as a column under this doc key
      if (field.isStored()) {

        byte[] _value = field.isBinary() ? field.getBinaryValue() : field
            .stringValue().getBytes();

        // last byte flags if binary or not
        byte[] value = new byte[_value.length + 1];
        System.arraycopy(_value, 0, value, 0, _value.length);

        value[value.length - 1] = (byte) (field.isBinary() ? 'B' : 'T');

        String key = docId;

        this.indexTransactionLog.storeField(Bytes.toBytes(key), field.name(),
            value);
      }
    }

    this.indexTransactionLog.commit();
  }

  // TODO: This method needs to be refactored to the NoSqlIndexWriter
  String createColumnName(final String fieldName, final String term) {
    return fieldName + "/" + term;
  }
}
