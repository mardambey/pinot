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
package com.linkedin.pinot.index.writer;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.Random;

import org.apache.commons.lang.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.core.io.reader.impl.FixedByteSingleValueMultiColReader;
import com.linkedin.pinot.core.io.writer.impl.FixedByteSingleValueMultiColWriter;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;


@Test
public class FixedByteWidthRowColDataFileWriterTest {
  @Test
  public void testSingleCol() throws Exception {

    File file = new File("test_single_col_writer.dat");
    file.delete();
    int rows = 100;
    int cols = 1;
    int[] columnSizes = new int[] { 4 };
    FixedByteSingleValueMultiColWriter writer = new FixedByteSingleValueMultiColWriter(
        file, rows, cols, columnSizes);
    int[] data = new int[rows];
    Random r = new Random();
    for (int i = 0; i < rows; i++) {
      data[i] = r.nextInt();
      writer.setInt(i, 0, data[i]);
    }
    writer.close();
    DataInputStream dis = new DataInputStream(new FileInputStream(file));
    for (int i = 0; i < rows; i++) {
      Assert.assertEquals(dis.readInt(), data[i]);
    }
    dis.close();
    file.delete();
  }

  @Test
  public void testMultiCol() throws Exception {

    File file = new File("test_single_col_writer.dat");
    file.delete();
    int rows = 100;
    int cols = 2;
    int[] columnSizes = new int[] { 4, 4 };
    FixedByteSingleValueMultiColWriter writer = new FixedByteSingleValueMultiColWriter(
        file, rows, cols, columnSizes);
    int[][] data = new int[rows][cols];
    Random r = new Random();
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < cols; j++) {
        data[i][j] = r.nextInt();
        writer.setInt(i, j, data[i][j]);
      }
    }
    writer.close();
    DataInputStream dis = new DataInputStream(new FileInputStream(file));
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < cols; j++) {
        Assert.assertEquals(dis.readInt(), data[i][j]);
      }
    }
    dis.close();
    file.delete();
  }

  @Test
  public void testSpecialCharsForStringReaderWriter() throws Exception {
    final byte[] bytes1 = new byte[] { -17, -65, -67, -17, -65, -67, 32, 69, 120, 101, 99, 117, 116, 105, 118, 101 };
    final byte[] bytes2 = new byte[] { -17, -65, -68, 32, 99, 97, 108, 103, 97, 114, 121, 32, 106, 117, 110, 107, 32, 114, 101, 109, 111, 118, 97, 108 };
    File file = new File("test_single_col_writer.dat");
    file.delete();
    int rows = 100;
    int cols = 1;
    String testString1 = new String(bytes1);
    String testString2 = new String(bytes2);
    System.out.println(Arrays.toString(bytes2));
    int stringColumnMaxLength = Math.max(testString1.getBytes().length, testString2.getBytes().length);
    int[] columnSizes = new int[] { stringColumnMaxLength };
    FixedByteSingleValueMultiColWriter writer = new FixedByteSingleValueMultiColWriter(file, rows, cols, columnSizes);
    String[] data = new String[rows];
    for (int i = 0; i < rows; i++) {
      String toPut = (i % 2 == 0) ? testString1 : testString2;
      final int padding = stringColumnMaxLength - toPut.getBytes().length;

      final StringBuilder bld = new StringBuilder();
      bld.append(toPut);
      for (int j = 0; j < padding; j++) {
        bld.append(V1Constants.Str.STRING_PAD_CHAR);
      }
      data[i] = bld.toString();
      writer.setString(i, 0, data[i]);
    }
    writer.close();
    FixedByteSingleValueMultiColReader dataFileReader = FixedByteSingleValueMultiColReader.forMmap(file, rows, 1, new int[] { stringColumnMaxLength });
    for (int i = 0; i < rows; i++) {
      String stringInFile = dataFileReader.getString(i, 0);
      Assert.assertEquals(stringInFile, data[i]);
      Assert.assertEquals(StringUtils.remove(stringInFile, String.valueOf(V1Constants.Str.STRING_PAD_CHAR)),
          StringUtils.remove(data[i], String.valueOf(V1Constants.Str.STRING_PAD_CHAR)));
    }
    file.delete();
  }
}
