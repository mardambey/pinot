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
package com.linkedin.pinot.routing.builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.SegmentNameBuilder;
import com.linkedin.pinot.routing.ServerToSegmentSetMap;


public class KafkaHighLevelConsumerBasedRoutingTableBuilder implements RoutingTableBuilder {

  @Override
  public void init(Configuration configuration) {
  }

  @Override
  public List<ServerToSegmentSetMap> computeRoutingTableFromExternalView(String tableName, ExternalView externalView,
      List<InstanceConfig> instanceConfigList) {

    RoutingTableInstancePruner pruner = new RoutingTableInstancePruner(instanceConfigList);

    Set<String> segments = externalView.getPartitionSet();
    List<ServerToSegmentSetMap> routingTable = new ArrayList<ServerToSegmentSetMap>();
    Map<String, Map<String, Set<String>>> groupIdToRouting = new HashMap<String, Map<String, Set<String>>>();
    for (String segment : segments) {
      Map<String, String> instanceMap = externalView.getStateMap(segment);
      for (String instance : instanceMap.keySet()) {
        if (!instanceMap.get(instance).equals(CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE)
            || pruner.isInactive(instance)) {
          continue;
        }

        String groupId = SegmentNameBuilder.Realtime.extractGroupIdName(segment);
        if (!groupIdToRouting.containsKey(groupId)) {
          groupIdToRouting.put(groupId, new HashMap<String, Set<String>>());
        }
        if (!groupIdToRouting.get(groupId).containsKey(instance)) {
          groupIdToRouting.get(groupId).put(instance, new HashSet<String>());
        }
        groupIdToRouting.get(groupId).get(instance).add(segment);
      }
    }
    for (Map<String, Set<String>> replicaRouting : groupIdToRouting.values()) {
      routingTable.add(new ServerToSegmentSetMap(replicaRouting));
    }
    return routingTable;
  }

}
