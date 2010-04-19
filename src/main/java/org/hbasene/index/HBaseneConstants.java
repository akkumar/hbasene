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

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Constants for the HBasene schema
 */
public interface HBaseneConstants {

  /**
   * Column Family representing the term vector of a given term ( Field:Term, a
   * field-term combination). The columns are usually the doc-ids, with the
   * values containing the actual term positions in them, if they were
   * available.
   */
  static final byte[] FAMILY_TERMVECTOR = Bytes.toBytes("fm.termVector");
  
  
  /**
   * Column Family representing the payloads associated with a given term for a given document.
   * 
   */
  static final byte[] FAMILY_PAYLOADS = Bytes.toBytes("fm.payloads");

  /**
   * Column family that contains the stored fields of Lucene Documents.
   * The columns are usually the field names with the values being the 
   * contents of the same ( in a byte array, hence compatible with both ASCII / binary formats).
   */
  static final byte[] FAMILY_FIELDS = Bytes.toBytes("fm.fields");

  /**
   * Column family that contains the mapping from the docId to an integer
   */
  static final byte[] FAMILY_DOC_TO_INT = Bytes.toBytes("fm.doc2int");

  /**
   * Qualifier belonging to family, {@link #FAMILY_DOC_TO_INT} , representing an
   * uniquely increasing integer used by Lucene for internal purposes.
   */
  static final byte[] QUALIFIER_INT = Bytes.toBytes("qual.Int");


  /**
   * Column family to store the sequence of the counter used by lucene.
   */
  static final byte[] FAMILY_SEQUENCE = Bytes.toBytes("fm.sequence");

  /**
   * Qualifier that represents a sequence.
   */
  static final byte[] QUALIFIER_SEQUENCE = Bytes.toBytes("qual.sequence");

  /**
   * Row Key for a special entry
   */
  static final byte[] ROW_SEQUENCE_ID = Bytes.toBytes("sequenceId");
  
}
