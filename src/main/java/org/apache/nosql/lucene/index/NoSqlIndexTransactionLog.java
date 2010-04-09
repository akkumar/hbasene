package org.apache.nosql.lucene.index;

import java.io.IOException;
import java.util.List;

/**
 * Transaction Log of (Lucene) Index operations. Rudimentary TF-IDF operations.
 * 
 * 
 */
public abstract class NoSqlIndexTransactionLog {

  /**
   * Initialize the log
   * 
   * @throws IOException
   */
  public abstract void init() throws IOException;

  /**
   * Close the log.
   * 
   * @throws IOException
   */
  public abstract void close() throws IOException;

  /**
   * Commit the transactions.
   * 
   * @throws IOException
   */
  public abstract void commit() throws IOException;

  /**
   * Adds term vectors for a given 'field/Term' combination.
   * 
   * @param docId
   * @param fieldTerm
   *          Field/Term combination
   * @param termVectors
   */
  public abstract void addTermVectors(final String fieldTerm, byte[] docId,
      List<Integer> termVectors);

  /**
   * Store the given field in the lucene hbase index.
   * 
   * @param key
   * @param value
   */
  public abstract void storeField(final byte[] docId, final String fieldName,
      byte[] value);
}
