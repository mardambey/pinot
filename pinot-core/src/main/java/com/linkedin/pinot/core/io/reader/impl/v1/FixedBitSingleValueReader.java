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
package com.linkedin.pinot.core.io.reader.impl.v1;

import java.io.File;
import java.io.IOException;

import com.linkedin.pinot.core.io.reader.BaseSingleColumnSingleValueReader;
import com.linkedin.pinot.core.io.reader.SingleColumnSingleValueReader;
import com.linkedin.pinot.core.io.reader.impl.FixedBitSingleValueMultiColReader;



public class FixedBitSingleValueReader extends BaseSingleColumnSingleValueReader {

  private final File indexFile;
  private final FixedBitSingleValueMultiColReader dataFileReader;
  private final int rows;

  public FixedBitSingleValueReader(File file, int rows, int columnSize, boolean isMMap, boolean hasNulls) throws IOException {
    indexFile = file;
    if (isMMap) {
      dataFileReader = FixedBitSingleValueMultiColReader.forMmap(indexFile, rows, 1, new int[] { columnSize }, new boolean[] { hasNulls });
    } else {
      dataFileReader = FixedBitSingleValueMultiColReader.forHeap(indexFile, rows, 1, new int[] { columnSize }, new boolean[] { hasNulls });
    }
    this.rows = rows;
  }

  public FixedBitSingleValueMultiColReader getDataFileReader() {
    return dataFileReader;
  }

  public int getLength() {
    return rows;
  }

  @Override
  public void close() throws IOException {
    dataFileReader.close();
  }

  @Override
  public int getInt(int row) {
    return dataFileReader.getInt(row, 0);
  }

}
