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

/**
 * Encode the term positions as bytes.
 */
public abstract class AbstractTermPositionsEncoder {

  /**
   * Encode the given term positions as a byte array.
   * 
   * @param termPositions
   * @return
   */
  abstract byte[] encode(final List<Integer> termPositions);

  /**
   * Decode the byte string represented by term positions and retrieve the
   * individual list.
   * 
   * @param value
   * @return
   */
  abstract int[] decode(final byte[] value);

  /**
   * Retrieve the frequency of the terms in the current document.
   * 
   * @param termFreqRepresentation
   * @return
   */
  abstract int getTermFrequency(byte[] termFreqRepresentation);
}
