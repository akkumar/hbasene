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

import com.google.common.base.Joiner;

/**
 * Encodes the term positions as a comma-delimiter string in ASCII format.
 */
public class AsciiTermPositionsEncoder extends AbstractTermPositionsEncoder {

  /**
   * Character used to join the elements of a term document array.
   */
  private static final char JOIN_CHAR = ',';

  @Override
  int[] decode(byte[] value) {
    if (value == null) {
      return new int[0];
    }
    final String tf = Bytes.toString(value);
    if (tf.length() == 0) {
      return new int[0];
    }
    String[] tfs = tf.split(",");
    int[] result = new int[tfs.length];
    for (int i = 0; i < tfs.length; ++i) {
      result[i] = Integer.valueOf(tfs[i]);
    }
    return result;

  }

  @Override
  byte[] encode(List<Integer> termPositions) {
    String tf = Joiner.on(JOIN_CHAR)
        .join(termPositions.toArray(new Integer[0]));
    return Bytes.toBytes(tf);
  }

  @Override
  int getTermFrequency(final byte[] termFreqRepresentation) {
    if (termFreqRepresentation == null) {
      return 0;
    }
    String tf = Bytes.toString(termFreqRepresentation);
    int count = 1;
    for (int i = 0; i < tf.length(); ++i) {
      if (tf.charAt(i) == JOIN_CHAR) {
        count++;
      }
    }
    return count;
  }

}
