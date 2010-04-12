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
package org.apache.hbasene.index;

import java.io.IOException;
import java.util.List;

/**
 * Transaction Log of (Lucene) Index operations. Rudimentary TF-IDF operations.
 * 
 * 
 */
public abstract class AbstractIndexTransactionLog {

  /**
   * Initialize the log
   * 
   * @throws IOException
   */
  public abstract void init() throws IOException;

  /**
   * Close the log.
   * 
   * @throws IOException
   */
  public abstract void close() throws IOException;

  /**
   * Commit the transactions.
   * 
   * @throws IOException
   */
  public abstract void commit() throws IOException;

  /**
   * Adds term vectors for a given 'field/Term' combination.
   * 
   * @param docId
   * @param fieldTerm
   *          Field/Term combination
   * @param termPositionVectors
   *          Term Position Vectors for the given fieldTerm , present in the
   *          given docId.
   */
  public abstract void addTermVectors(final String fieldTerm, byte[] docId,
      final List<Integer> termPositionVectors);

  /**
   * Store the given field in the lucene hbase index.
   * 
   * @param key
   * @param value
   */
  public abstract void storeField(final byte[] docId, final String fieldName,
      byte[] value);

  /**
   * Assign a docId to the given primary key in the Lucene schema.
   * 
   * @param primaryKey
   * @return correct docId, if in place. -1, otherwise.
   * @throws IOException
   */
  public abstract long assignDocId(byte[] primaryKey) throws IOException;
}
