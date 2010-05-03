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
package org.hbasene.index;

import java.io.IOException;

/**
 * Transaction Log of (Lucene) Index operations. Rudimentary TF-IDF operations.
 * 
 * 
 */
public abstract class AbstractIndexStore {

  /**
   * Commit the transactions.
   * 
   * @throws IOException
   */
  public abstract void commit() throws IOException;

  /**
   * Close the transactions.
   * 
   * @throws IOException
   */
  public abstract void close() throws IOException;

  /**
   * Index a given document.
   * 
   * @param key
   * @param documentIndexContext
   * @return SegmentInfo that contains a segment id and a document id.
   * @throws IOException
   */
  public abstract SegmentInfo indexDocument(final String key,
      final DocumentIndexContext documentIndexContext) throws IOException;

}
