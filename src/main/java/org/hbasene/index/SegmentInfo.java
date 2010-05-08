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
 * 
 */
package org.hbasene.index;

/**
 * Information about a given segment, returned by the store after inserting a document into the store.
 * 
 */
public class SegmentInfo {

  private final int documentId;
  
  private final long segmentId;
  
  public SegmentInfo( final long segmentId, final int documentId) { 
    this.documentId = documentId;
    this.segmentId = segmentId;
  }
  
  /**
   * The segment of the given document in the store.
   * @return
   */
  public long getSegmentId() { 
    return this.segmentId;
  }
  
  /**
   * The document Id in a given segment.
   * @return
   */
  public int getDocumentId() { 
    return this.documentId;
  }
}
