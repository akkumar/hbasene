package org.hbasene.index;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import jsr166y.RecursiveAction;

import org.apache.lucene.util.OpenBitSet;

public class TermVectorAppendTask extends RecursiveAction {

  static final int THRESHOLD = 500;

  private final int termVectorArrayThreshold;

  private final long docBase;

  private final String[] inputTerms;

  private final long docId;

  private final ConcurrentHashMap<String, Object> termVectorMap;

  private final int low;

  private final int high;

  public TermVectorAppendTask(final String[] inputTerms, int low, int high,
      final long docId, final ConcurrentHashMap<String, Object> termVectorMap,
      final int termVectorArrayThreshold, final long docBase) {
    this.docId = docId;
    this.inputTerms = inputTerms;
    this.termVectorMap = termVectorMap;
    this.low = low;
    this.high = high;
    this.termVectorArrayThreshold = termVectorArrayThreshold;
    this.docBase = docBase;
  }

  @Override
  protected void compute() {
    if ((high - low) < THRESHOLD) {
      processAssign();
    } else {
      int mid = (low + high) >>> 1;

      invokeAll(new TermVectorAppendTask(this.inputTerms, low, mid, docId,
          this.termVectorMap, this.termVectorArrayThreshold, this.docBase),
          new TermVectorAppendTask(this.inputTerms, mid, high, docId,
              this.termVectorMap, this.termVectorArrayThreshold, this.docBase));

    }
  }

  void processAssign() {
    for (int i = low; i < high; ++i) {
      final String fieldTerm = this.inputTerms[i];
      Object docs = this.termVectorMap.get(fieldTerm);
      if (docs == null) {
        docs = new ArrayList<Integer>();
        this.termVectorMap.put(fieldTerm, docs);
      }
      long relativeId = docId - docBase;
      if (docs instanceof List) {
        List<Integer> listImpl = (List<Integer>) docs;
        listImpl.add((int) relativeId);
        if (listImpl.size() > this.termVectorArrayThreshold) {
          OpenBitSet bitset = new OpenBitSet();
          for (Integer value : listImpl) {
            bitset.set(value);
          }
          listImpl.clear();
          this.termVectorMap.put(fieldTerm, bitset);
        }
      } else if (docs instanceof OpenBitSet) {
        ((OpenBitSet) docs).set(relativeId);
      }
    }
  }
}
