package org.hbasene.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.util.Version;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHBaseIndexWriter {

  private static final Logger LOGGER = Logger
  .getLogger(TestHBaseIndexWriter.class.getName());
  
  static HBaseIndexWriter writer;

  static MyIndexStore store;
  
  @BeforeClass
  public static void setUp() throws IOException {
    store = new MyIndexStore();
    writer = new HBaseIndexWriter (store, "id");
  }

  @AfterClass
  public static void tearDown() {

  }

  @Test
  public void testWrite() throws CorruptIndexException, IOException {
    Document doc  = new Document();
    doc.add(new Field("content", "Quick Brown Fox Jumped over the bridge", Field.Store.NO,
       Field.Index.ANALYZED_NO_NORMS));
    doc.add(new Field("id", "myrow", Field.Store.YES,
       Field.Index.NOT_ANALYZED_NO_NORMS));
   
    writer.addDocument(doc, new StandardAnalyzer(Version.LUCENE_30));
    boolean contentPresent = false;
    for (final String term : store.tfs.keySet()) { 
      if (term.startsWith("content/")) {
        contentPresent = true;
        break;
      }
    }
    Assert.assertTrue("Content term is present", contentPresent);
    LOGGER.info(store.tfs.keySet());
    
  }

  static final class MyIndexStore extends AbstractIndexStore {

    Map<String, List<Long>> tfs = new HashMap<String, List<Long>>();
    

    @Override
    public void close() throws IOException {
      // TODO Auto-generated method stub

    }

    @Override
    public void commit() throws IOException {
      // TODO Auto-generated method stub

    }

  

    @Override
    public SegmentInfo indexDocument(String key,
        DocumentIndexContext documentIndexContext) throws IOException {
      // TODO Auto-generated method stub
      return null;
    }

  }
}
