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

import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Binary encoding of the term positions and constant time retrieval,
 * for the term frequencies
 */
public class AlphaTermPositionsEncoder extends AbstractTermPositionsEncoder {

  @Override
  int[] decode(byte[] value) {
    int length = (value.length / Bytes.SIZEOF_INT);
    int [] result = new int[length];
    for (int i = 0 ; i < length ; ++i) {
      result[i] = Bytes.toInt(value, i * Bytes.SIZEOF_INT);
    }
    return result;
  }

  @Override
  byte[] encode(List<Integer> termPositions) {
    byte[] encoded = new byte[termPositions.size() * Bytes.SIZEOF_INT];
    for (int i = 0 ; i < termPositions.size() ; ++i) {
      Bytes.putInt(encoded, i * Bytes.SIZEOF_INT, termPositions.get(i));
    }
    return encoded;
  }

  @Override
  int getTermFrequency(byte[] termPositions) {
    return (termPositions.length / Bytes.SIZEOF_INT);
  }

}
