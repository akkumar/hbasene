package org.hbasene.index;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import jsr166y.RecursiveAction;

import org.apache.lucene.util.OpenBitSet;

public class TermVectorAppendTask extends RecursiveAction {

  static final int THRESHOLD = 500;

  private final int termVectorArrayThreshold;

  //private final String[] inputTerms;

  private final long relativeId;

  private final ConcurrentHashMap<String, Object> termVectorMap;

  //private final int low;

  //private final int high;

  public TermVectorAppendTask(final String[] inputTerms, int low, int high,
      final ConcurrentHashMap<String, Object> termVectorMap,
      final int termVectorArrayThreshold, final long relativeId) {
    //this.inputTerms = inputTerms;
    this.termVectorMap = termVectorMap;
    //this.low = low;
    //this.high = high;
    this.termVectorArrayThreshold = termVectorArrayThreshold;
    this.relativeId = relativeId;
  }

  @Override
  protected void compute() { }
  /**
  @Override
  protected void compute() {
    if ((high - low) < THRESHOLD) {
      for (int i = low; i < high; ++i) {
        processAssign(this.inputTerms[i]);
      }
    } else {
      int mid = (low + high) >>> 1;

      invokeAll(new TermVectorAppendTask(this.inputTerms, low, mid, 
          this.termVectorMap, this.termVectorArrayThreshold, this.relativeId),
          new TermVectorAppendTask(this.inputTerms, mid, high, 
              this.termVectorMap, this.termVectorArrayThreshold, this.relativeId));

    }
  }
  **/

  void processAssign(final String fieldTerm) {
    Object docs = this.termVectorMap.get(fieldTerm);
    if (docs == null) {
      docs = new ArrayList<Integer>();
      this.termVectorMap.put(fieldTerm, docs);
    }
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
