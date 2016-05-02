/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.filter;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.operator.blocks.BaseFilterBlock;
import com.linkedin.pinot.core.operator.blocks.BitmapBlock;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluator;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import com.linkedin.pinot.core.segment.index.readers.InvertedIndexReader;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public class BitmapBasedFilterOperator extends BaseFilterOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(BitmapBasedFilterOperator.class);

  private DataSource dataSource;
  private BitmapBlock bitmapBlock;

  private int startDocId;

  private int endDocId;

  /**
   * 
   * @param dataSource
   * @param startDocId inclusive
   * @param endDocId inclusive
   */
  public BitmapBasedFilterOperator(DataSource dataSource, int startDocId, int endDocId) {
    this.dataSource = dataSource;
    this.startDocId = startDocId;
    this.endDocId = endDocId;
  }

  @Override
  public boolean open() {
    return true;
  }

  @Override
  public BaseFilterBlock nextFilterBlock(BlockId BlockId) {
    Predicate predicate = getPredicate();
    InvertedIndexReader invertedIndex = dataSource.getInvertedIndex();
    Block dataSourceBlock = dataSource.nextBlock();
    Dictionary dictionary = dataSource.getDictionary();
    PredicateEvaluator evaluator = PredicateEvaluatorProvider.getPredicateFunctionFor(predicate, dictionary);
    int[] dictionaryIds;
    boolean exclusion = false;
    switch (predicate.getType()) {
      case EQ:
      case IN:
      case RANGE:
        dictionaryIds = evaluator.getMatchingDictionaryIds();
        break;

      case NEQ:
      case NOT_IN:
        exclusion = true;
        dictionaryIds = evaluator.getNonMatchingDictionaryIds();
        break;
      case REGEX:
      default:
        throw new UnsupportedOperationException("Regex is not supported");
    }
    ImmutableRoaringBitmap[] bitmaps = new ImmutableRoaringBitmap[dictionaryIds.length];
    for (int i = 0; i < dictionaryIds.length; i++) {
      bitmaps[i] = invertedIndex.getImmutable(dictionaryIds[i]);
    }
    bitmapBlock = new BitmapBlock(dataSource.getOperatorName(), dataSourceBlock.getMetadata(), startDocId, endDocId, bitmaps, exclusion);
    return bitmapBlock;
  }

  @Override
  public boolean close() {
    return true;
  }

}
