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
package org.hbasene.index.util;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.util.OpenBitSet;

public class HBaseneUtil {


  
  public static final String QUALIFIER_DOCUMENTS_PREFIX = "qual.documents";  
  
  public static final byte[] FAMILY_TERMVECTOR = Bytes.toBytes("fm.termVector");

  /**
   * Serialization library
   */
  private HBaseneUtil() { 
    
  }
  
  /**
   * Convert the given OpenBitSet to a serialized byte array.
   * @param bitset
   * @return Bytes representation of the OpenBitSet under consideration
   */
  public static byte[] toBytes(final OpenBitSet bitset) { 
    long [] bits = bitset.getBits();
    int wlen = bitset.getNumWords();
    byte[] output = new byte[wlen * Bytes.SIZEOF_LONG];
    for (int i = 0; i < wlen; ++i) {
      Bytes.putBytes(output, i * Bytes.SIZEOF_LONG, Bytes.toBytes(bits[i]), 0, Bytes.SIZEOF_LONG);
    }
    return output;
  }
  
  
  /**
   * Convert the given byte array to an open bitset.
   * @param bytes
   * @return Convert from bytes to OpenBitSet
   **/
  public static OpenBitSet toOpenBitSet(final byte[] bytes) {
    int wlen = bytes.length / Bytes.SIZEOF_LONG ;
    if (wlen == 0) { 
      return null;
    }
    long [] bits = new long[wlen];
    for (int i = 0; i < wlen ; ++i) {
      bits[i] = Bytes.toLong(bytes, i * Bytes.SIZEOF_LONG);
    }
    return new OpenBitSet(bits, wlen);
  }

  
  /**
   * Create the default openBitSet.
   * @return Default openBitSet of a small document size.
   */
  public static OpenBitSet createDefaultOpenBitSet() { 
    return createDefaultOpenBitSet(65); //By default - allocate 1 word  
  }

  /**
   * Create the default openBitSet.
   * @return Default openBitSet of a small document size.
   */
  public static OpenBitSet createDefaultOpenBitSet(long numbits) { 
    return new OpenBitSet(numbits); 
  }

  
  public static byte[] createTermVectorQualifier(int partitionId) {
    return Bytes.toBytes(QUALIFIER_DOCUMENTS_PREFIX);
  }
   
  public static final int MAX_DOCS = 10000;

}
