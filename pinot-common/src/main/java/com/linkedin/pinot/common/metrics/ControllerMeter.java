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
package com.linkedin.pinot.common.metrics;

import com.linkedin.pinot.common.Utils;


/**
 * Meters for the controller.
 */
public enum ControllerMeter implements AbstractMetrics.Meter {
  HELIX_ZOOKEEPER_RECONNECTS("reconnects", true);

  private final String brokerMeterName;
  private final String unit;
  private final boolean global;

  ControllerMeter(String unit, boolean global) {
    this.unit = unit;
    this.global = global;
    this.brokerMeterName = Utils.toCamelCase(name().toLowerCase());
  }

  @Override
  public String getMeterName() {
    return brokerMeterName;
  }

  @Override
  public String getUnit() {
    return unit;
  }

  /**
   * Returns true if the metric is global (not attached to a particular resource)
   *
   * @return true if the metric is global
   */
  @Override
  public boolean isGlobal() {
    return global;
  }
}
