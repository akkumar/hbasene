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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Table Pool, with autoFlush set to false, for client-side write buffering, for
 * better performance.
 */
public class IndexHTablePool extends HTablePool {

  private final HBaseConfiguration conf;

  private final int maxSize;

  private final Map<String, BlockingQueue<HTable>> tables = Collections
      .synchronizedMap(new HashMap<String, BlockingQueue<HTable>>());

  public IndexHTablePool(final HBaseConfiguration conf, int maxSize) {
    super(conf, maxSize);
    this.conf = conf;
    this.maxSize = maxSize;
  }

  /**
   * @param tableName
   * @return HTable instance.
   * @deprecated Use createHTable
   */
  @Override
  protected HTable newHTable(String tableName) {
    try {
      HTable table = new HTable(conf, Bytes.toBytes(tableName));
      table.setAutoFlush(false); // client throughput
      return table;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Get a reference to the specified table from the pool.
   * <p>
   * 
   * Create a new one if one is not available.
   * 
   * @param tableName
   * @return a reference to the specified table
   * @throws RuntimeException
   *           if there is a problem instantiating the HTable
   */
  @Override
  public HTable getTable(String tableName) {
    BlockingQueue<HTable> queue = tables.get(tableName);
    if (queue == null) {
      synchronized (tables) {
        queue = tables.get(tableName);
        if (queue == null) {
          queue = new LinkedBlockingQueue<HTable>(this.maxSize);
          for (int i = 0; i < this.maxSize; ++i) {
            queue.add(this.newHTable(tableName));
          }
          tables.put(tableName, queue);
        }
      }
    }
    try {
      return queue.take();
    } catch (Exception ex) {
      return null;
    }
  }

  /**
   * Get a reference to the specified table from the pool.
   * <p>
   * 
   * Create a new one if one is not available.
   * 
   * @param tableName
   * @return a reference to the specified table
   * @throws RuntimeException
   *           if there is a problem instantiating the HTable
   */
  public HTable getTable(byte[] tableName) {
    return getTable(Bytes.toString(tableName));
  }

  /**
   * Puts the specified HTable back into the pool.
   * <p>
   * 
   * If the pool already contains <i>maxSize</i> references to the table, then
   * nothing happens.
   * 
   * @param table
   */
  @Override
  public void putTable(HTable table) {
    BlockingQueue<HTable> queue = tables.get(Bytes.toString(table
        .getTableName()));
    try {
      queue.put(table);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
