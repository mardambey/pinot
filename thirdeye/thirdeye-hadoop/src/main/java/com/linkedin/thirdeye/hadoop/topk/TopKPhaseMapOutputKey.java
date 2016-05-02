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
package com.linkedin.thirdeye.hadoop.topk;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Wrapper for the key generated by mapper in TopKPhase
 */
public class TopKPhaseMapOutputKey {

  String dimensionName;
  String dimensionValue;

  public TopKPhaseMapOutputKey(String dimensionName, String dimensionValue) {
    this.dimensionName = dimensionName;
    this.dimensionValue = dimensionValue;
  }

  public String getDimensionName() {
    return dimensionName;
  }

  public String getDimensionValue() {
    return dimensionValue;
  }

  public byte[] toBytes() throws IOException {

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    byte[] bytes;
    // dimension name
    bytes = dimensionName.getBytes();
    dos.writeInt(bytes.length);
    dos.write(bytes);
    // dimension value
    bytes = dimensionValue.getBytes();
    dos.writeInt(bytes.length);
    dos.write(bytes);

    baos.close();
    dos.close();
    return baos.toByteArray();
  }

  public static TopKPhaseMapOutputKey fromBytes(byte[] buffer) throws IOException {
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(buffer));
    int length;
    byte[] bytes;
    // dimension name
    length = dis.readInt();
    bytes = new byte[length];
    dis.read(bytes);
    String dimensionName = new String(bytes);
    // dimension value
    length = dis.readInt();
    bytes = new byte[length];
    dis.read(bytes);
    String dimensionValue = new String(bytes);

    TopKPhaseMapOutputKey wrapper;
    wrapper = new TopKPhaseMapOutputKey(dimensionName, dimensionValue);
    return wrapper;
  }

}
