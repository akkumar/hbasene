/**
 * Copyright 2010 Karthik Kumar
 *
 * Based off the original code by Lucandra project, (C): Jake Luciani
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

public class HBaseneConfiguration {

  /**
   * Maximum Term Vector Size ( in MB )
   */
  public static final String CONF_MAX_TERM_VECTOR = "max.term.vector.in.mb";
  
  
  /**
   * The numeric limit beyond which the representation of a term vector becomes a
   * bitset, ( as opposed to a list, by default).
   */
  public static final String CONF_TERM_VECTOR_LIST_THRESHOLD = "term.vector.list.threshold";

}
