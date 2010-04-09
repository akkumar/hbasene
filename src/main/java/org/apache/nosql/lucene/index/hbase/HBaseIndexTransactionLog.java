package org.apache.nosql.lucene.index.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.nosql.lucene.index.NoSqlIndexTransactionLog;

import com.google.common.base.Joiner;

/**
 * An index formed on-top of HBase. This requires a table with the following
 * column families.
 * 
 * <ul>
 * <li>termVector</li>
 * <li>documents</li>
 * </ul>
 * 
 * To create a HBase Table, specific to the index schema, refer to
 * {@link #createLuceneIndexTable(String, HBaseConfiguration, boolean)} .
 */
public class HBaseIndexTransactionLog extends NoSqlIndexTransactionLog {

  /**
   * Column Family representing the term vector of a given term ( Field:Term, a
   * field-term combination). The columns are usually the doc-ids, with the
   * values containing the actual term positions in them, if they were
   * available.
   */
  static final byte[] FAMILY_TERM_VECTOR = Bytes.toBytes("termVector");

  /**
   * Column family that contains the document's stored content. The columns are
   * usually the field names with the values being the contents of the same.
   */
  static final byte[] FAMILY_DOCUMENTS = Bytes.toBytes("documents");

  /**
   * Character used to join the elements of a term document array.
   */
  private static final char JOIN_CHAR = ',';

  /**
   * List of puts to go in.
   */
  private List<Put> puts;

  private final Configuration configuration;

  /**
   * The name of the lucene index under consideration.
   */
  private final String indexName;

  /**
   * Table instance under consideration.
   */
  private HTable table;

  public HBaseIndexTransactionLog(final Configuration configuration,
      final String indexName) {
    this.puts = new ArrayList<Put>();
    this.indexName = indexName;
    this.configuration = configuration;
  }

  public String getIndexName() {
    return this.indexName;
  }

  @Override
  public void init() throws IOException {
    this.table = createLuceneIndexTable(indexName, configuration, false);
    this.table.setAutoFlush(false);
  }

  @Override
  public void close() throws IOException {
    this.table.close();
  }

  @Override
  public void commit() throws IOException {
    this.table.put(puts);
    this.table.flushCommits();
    puts.clear();
  }

  @Override
  public void addTermVectors(String fieldTerm, byte[] docId,
      List<Integer> termVectors) {
    Put put = new Put(Bytes.toBytes(fieldTerm));
    put.add(FAMILY_TERM_VECTOR, docId, toBytes(termVectors));
    this.puts.add(put);
  }

  @Override
  public void storeField(byte[] docId, String fieldName, byte[] value) {
    Put put = new Put(docId);
    put.add(FAMILY_DOCUMENTS, Bytes.toBytes(fieldName), value);
    this.puts.add(put);
  }

  static byte[] toBytes(final Integer[] array) {
    String tf = Joiner.on(JOIN_CHAR).join(array);
    return Bytes.toBytes(tf);
    // TODO: Rudimentary implementation of a comma-separated join for the
    // representation of integer array in place.
    // A better encoding algorithm for encoding the document Ids might be
    // useful and space efficient.
  }

  static int getTermFrequency(final byte[] termFreqRepresentation) {
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

  static byte[] toBytes(final List<Integer> array) {
    return toBytes(array.toArray(new Integer[0]));
  }

  /**
   * Drop the given Lucene index table.
   * 
   * @param tableName
   * @param configuration
   * @throws IOException
   */
  public static void dropLuceneIndexTable(final String tableName,
      final Configuration configuration) throws IOException {
    HBaseAdmin admin = new HBaseAdmin(configuration);
    doDropTable(admin, tableName);
  }

  static void doDropTable(final HBaseAdmin admin, final String tableName)
      throws IOException {
    // TODO: The set of operations below are not atomic at all / Currently such
    // guarantee is not provided by HBase. Need to modify HBase RPC/ submit a
    // patch to incorporate the same.
    if (admin.tableExists(tableName)) {
      if (admin.isTableAvailable(tableName)) {
        admin.disableTable(tableName);
      }
      admin.deleteTable(tableName);
    }
  }

  /**
   * Create a table to store lucene indices, with the given name and the
   * configuration.
   * 
   * @param tableName
   *          Name of the table to hold lucene indices.
   * @param configuration
   *          Configuration to hold HBase schema.
   * @param forceRecreate
   *          Drop any old table if it exists by the same name.
   * @return a valid HTable reference to the table of the name, if created
   *         successfully. <br>
   *         null, if table was not created successfully.
   * @throws IOException
   *           in case of any error with regard to the same.
   */
  public static HTable createLuceneIndexTable(final String tableName,
      final Configuration configuration, boolean forceRecreate)
      throws IOException {
    HBaseAdmin admin = new HBaseAdmin(configuration);

    if (admin.tableExists(tableName)) {
      if (!forceRecreate) {
        throw new IllegalArgumentException(
            "Table already exists by the index name " + tableName);
      } else {
        doDropTable(admin, tableName);
      }
    }

    HTableDescriptor tableDescriptor = new HTableDescriptor(Bytes
        .toBytes(tableName));
    tableDescriptor.addFamily(createUniversionLZO(admin,
        HBaseIndexTransactionLog.FAMILY_DOCUMENTS));
    tableDescriptor.addFamily(createUniversionLZO(admin,
        HBaseIndexTransactionLog.FAMILY_TERM_VECTOR));

    admin.createTable(tableDescriptor);
    HTableDescriptor descriptor = admin.getTableDescriptor(Bytes
        .toBytes(tableName));
    return (descriptor != null) ? new HTable(configuration, tableName) : null;
  }

  static HColumnDescriptor createUniversionLZO(final HBaseAdmin admin,
      final byte[] columnFamilyName) {
    HColumnDescriptor desc = new HColumnDescriptor(columnFamilyName);
    // TODO: Is there anyway to check the algorithms supported by HBase in the
    // admin interface ?
    // if (admin.isSupported(Algorithm.LZO)) {
    // desc.setCompressionType(Algorithm.LZO);
    // }
    desc.setMaxVersions(1);
    return desc;
  }
}
