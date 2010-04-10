package org.apache.nosql.lucene.index.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;

/**
 * Term Docs implementation for HBase.
 */
public class HBaseTermDocs implements TermDocs {

  private final HTable table;

  /**
   * List of documents corresponding to the term docs under consideration.
   */
  private List<byte[]> documents;
  // TODO:WeakRef this and load on demand, if taken away, to save memory.

  /**
   * Current index into the documents array.
   */
  private int currentIndex;

  private byte[] currentRow;

  private Comparator<byte[]> INT_COMPARATOR = new Comparator<byte[]>() {

    @Override
    public int compare(byte[] o1, byte[] o2) {
      int lhs = Bytes.toInt(o1);
      int rhs = Bytes.toInt(o2);
      if (lhs < rhs)
        return -1;
      else if (lhs > rhs)
        return 1;
      else
        return 0;

    }

  };

  public HBaseTermDocs(final Configuration conf, final String indexName)
      throws IOException {
    this.table = new HTable(conf, indexName);
  }

  @Override
  public void close() throws IOException {
    documents.clear();
    currentIndex = 0;
  }

  @Override
  public int doc() {
    return Bytes.toInt(this.documents.get(this.currentIndex));
  }

  @Override
  public int freq() {
    try {
      Get get = new Get(this.currentRow);
      get.addColumn(HBaseIndexTransactionLog.FAMILY_TERM_VECTOR, this.documents
          .get(this.currentIndex));
      Result result = table.get(get);
      byte[] tfArray = result.getValue(
          HBaseIndexTransactionLog.FAMILY_TERM_VECTOR, this.documents
              .get(this.currentIndex));
      return HBaseIndexTransactionLog.getTermFrequency(tfArray);
    } catch (Exception ex) {
      return 0;
    }
  }

  @Override
  public boolean next() throws IOException {
    if (currentIndex < this.documents.size()) {
      this.currentIndex++;
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
    NavigableMap<byte[], byte[]> map = result
        .getFamilyMap(HBaseIndexTransactionLog.FAMILY_TERM_VECTOR);
    this.documents = new ArrayList<byte[]>(map.keySet());
    Collections.sort(documents, INT_COMPARATOR);
    this.currentIndex = 0;
  }

  Result getRowWithTermVectors() throws IOException {
    Get get = new Get(this.currentRow);
    get.addFamily(HBaseIndexTransactionLog.FAMILY_TERM_VECTOR);
    return this.table.get(get);
  }

  @Override
  public void seek(TermEnum termEnum) throws IOException {
    seek(termEnum.term());
  }

  @Override
  public boolean skipTo(int target) throws IOException {
    // TODO: Should the starting Index of the loop be 0 or currentIndex ?
    for (int i = 0; i < this.documents.size(); ++i) {
      if (Bytes.toInt(this.documents.get(i)) >= target) {
        currentIndex = i;
        return true;
      }
    }
    return false;
  }

}
