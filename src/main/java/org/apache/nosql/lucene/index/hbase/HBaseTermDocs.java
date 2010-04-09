package org.apache.nosql.lucene.index.hbase;

import java.io.IOException;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.NoSuchElementException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;

/**
 * 
 */
public class HBaseTermDocs implements TermDocs {

  private final HTable table;

  /**
   * Current result of the term under consideration.
   */
  private Iterator<KeyValue> itKeyValue;

  private KeyValue currentKV;

  private byte[] currentRow;

  public HBaseTermDocs(final Configuration conf, final String indexName)
      throws IOException {
    this.table = new HTable(conf, indexName);
  }

  @Override
  public void close() throws IOException {
    // Do nothing on close.
  }

  @Override
  public int doc() {
    return currentKV.getQualifier().length;
    // TODO: Better representation of docId need to be in place.
  }

  @Override
  public int freq() {
    return HBaseIndexTransactionLog.getTermFrequency(currentKV.getValue());
  }

  @Override
  public boolean next() throws IOException {
    if (itKeyValue.hasNext()) {

      currentKV = itKeyValue.next();
      return true;
    } else {
      return false;
    }
  }

  @Override
  public int read(int[] docs, int[] freqs) throws IOException {
    int count = 0;
    for (int i = 0; i < docs.length; ++i) {
      if (next()) {
        docs[i] = this.doc();
        freqs[i] = this.freq();
        ++count;
      } else {
        break;
      }
    }
    return count;
  }

  @Override
  public void seek(Term term) throws IOException {
    final String rowKey = term.field() + "/" + term.text();
    this.currentRow = Bytes.toBytes(rowKey);
    Result result = this.getRowWithTermVectors();
    this.itKeyValue = result.list().iterator();
  }

  Result getRowWithTermVectors() throws IOException {
    Get get = new Get(this.currentRow);
    get.addFamily(HBaseIndexTransactionLog.FAMILY_TERM_VECTOR);
    return this.table.get(get);
  }

  @Override
  public void seek(TermEnum termEnum) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean skipTo(int target) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

}
