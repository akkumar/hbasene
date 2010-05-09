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

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import jsr166y.RecursiveAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.util.OpenBitSet;
import org.hbasene.index.util.HBaseneUtil;

/**
 * Recursive task to generate a bunch of puts before inserting in.
 */
public class TermVectorPutTask extends RecursiveAction {

  private static final Log LOG = LogFactory.getLog(TermVectorPutTask.class);

  private static final int THRESHOLD = 500;

  //private final String[] inputTerms;

  //private final int low;

  //private final int high;

  private final long docBase;

  private final ConcurrentHashMap<String, Object> termVectorMap;

  //private final BlockingQueue<Put> queuePuts;

  public TermVectorPutTask(final String[] inputTerms, int low, int high,
      final ConcurrentHashMap<String, Object> termVectorMap,
      final long docBase, final BlockingQueue<Put> queuePuts) {
    //this.inputTerms = inputTerms;
    //this.low = low;
    //this.high = high;
    this.termVectorMap = termVectorMap;
    this.docBase = docBase;
    //this.queuePuts = queuePuts;
  }

  @Override
  protected void compute() { }
 /** 
    if ((high - low) < THRESHOLD) {
      try {
        for (int i = low; i < high; ++i) {
          generatePut(this.inputTerms[i]);
        }
      } catch (final Exception ex) {
        LOG.error("Error encountered while generating puts ", ex);
        ex.printStackTrace();
      }
    } else {
      if (low < 0 || high < 0) {
        return;
      }
      int mid = (low + high) >>> 1;

      invokeAll(new TermVectorPutTask(this.inputTerms, low, mid,
          this.termVectorMap, this.docBase, this.queuePuts),
          new TermVectorPutTask(this.inputTerms, mid, high, this.termVectorMap,
              this.docBase, this.queuePuts));

    }
  }  **/
  

  Put generatePut(final String key) {
    final Object value = this.termVectorMap.get(key);
    Put put = new Put(Bytes.toBytes(key));
    byte[] docSet = null;
    if (value instanceof OpenBitSet) {
      docSet = Bytes.add(Bytes.toBytes('O'), HBaseneUtil
          .toBytes((OpenBitSet) value));
      // TODO: Scope for optimization, Avoid the redundant array creation and
      // copying.
    } else if (value instanceof List) {
      List<Integer> list = (List<Integer>) value;
      byte[] out = new byte[(list.size() + 1) * Bytes.SIZEOF_INT];
      Bytes.putInt(out, 0, list.size());
      for (int i = 0; i < list.size(); ++i) {
        Bytes.putInt(out, (i + 1) * Bytes.SIZEOF_INT, list.get(i).intValue());
      }
      docSet = Bytes.add(Bytes.toBytes('A'), out);
    }
    put.add(HBaseneConstants.FAMILY_TERMVECTOR, Bytes.toBytes(this.docBase),
        docSet);
    put.setWriteToWAL(true);
    return put;
  }

}
