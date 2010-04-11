package org.apache.hbasene.index;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHBaseIndexReader extends AbstractHBaseneTest {

  private static IndexReader reader;

  @BeforeClass
  public static void setUp() {
    reader = new HBaseIndexReader(conf, TEST_INDEX);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    reader.close();
  }

  @Test
  public void testSearch() throws IOException {
    IndexSearcher searcher = new IndexSearcher(reader);
    TopDocs docs = searcher.search(new TermQuery(new Term("content", "plays")),
        3);
    Assert.assertTrue("At least 3 terms with the keyword plays available",
        docs.totalHits > 3);
    for (ScoreDoc doc : docs.scoreDocs) {
      Assert.assertTrue("Doc Id  " + doc.doc + " is >= 0", doc.doc >= 0); // valid
                                                                          // docId
      Assert.assertTrue("Score " + doc.score + " > 0.0f", doc.score > 0.0f); // valid
                                                                             // Score
    }
  }
}
