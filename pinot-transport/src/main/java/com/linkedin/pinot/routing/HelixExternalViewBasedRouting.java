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
package com.linkedin.pinot.routing;

import com.linkedin.pinot.common.utils.NetUtil;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.routing.builder.BalancedRandomRoutingTableBuilder;
import com.linkedin.pinot.routing.builder.KafkaHighLevelConsumerBasedRoutingTableBuilder;
import com.linkedin.pinot.routing.builder.RoutingTableBuilder;
import com.linkedin.pinot.transport.common.SegmentIdSet;


/**
 * HelixExternalViewBasedRouting will maintain the routing table for assigned data table.
 *
 *
 */
public class HelixExternalViewBasedRouting implements RoutingTable {

  private static final Logger LOGGER = LoggerFactory.getLogger(HelixExternalViewBasedRouting.class);
  private final Set<String> _dataTableSet = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
  private final RoutingTableBuilder _defaultOfflineRoutingTableBuilder;
  private final RoutingTableBuilder _defaultRealtimeRoutingTableBuilder;
  private final Map<String, RoutingTableBuilder> _routingTableBuilderMap;

  private final Map<String, List<ServerToSegmentSetMap>> _brokerRoutingTable =
      new ConcurrentHashMap<String, List<ServerToSegmentSetMap>>();
  private final Map<String, Integer> _routingTableLastKnownZkVersionMap = new ConcurrentHashMap<>();
  private final Random _random = new Random(System.currentTimeMillis());
  private final HelixExternalViewBasedTimeBoundaryService _timeBoundaryService;

  public HelixExternalViewBasedRouting(RoutingTableBuilder defaultOfflineRoutingTableBuilder,
      RoutingTableBuilder defaultRealtimeRoutingTableBuilder, Map<String, RoutingTableBuilder> routingTableBuilderMap,
      ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _timeBoundaryService = new HelixExternalViewBasedTimeBoundaryService(propertyStore);
    if (defaultOfflineRoutingTableBuilder != null) {
      _defaultOfflineRoutingTableBuilder = defaultOfflineRoutingTableBuilder;
    } else {
      _defaultOfflineRoutingTableBuilder = new BalancedRandomRoutingTableBuilder();
    }
    if (defaultRealtimeRoutingTableBuilder != null) {
      _defaultRealtimeRoutingTableBuilder = defaultRealtimeRoutingTableBuilder;
    } else {
      _defaultRealtimeRoutingTableBuilder = new KafkaHighLevelConsumerBasedRoutingTableBuilder();
    }
    if (routingTableBuilderMap != null) {
      _routingTableBuilderMap = routingTableBuilderMap;
    } else {
      _routingTableBuilderMap = new HashMap<String, RoutingTableBuilder>();
    }
  }

  @Override
  public Map<ServerInstance, SegmentIdSet> findServers(RoutingTableLookupRequest request) {
    String tableName = request.getTableName();

    if ((_brokerRoutingTable == null) || (!_brokerRoutingTable.containsKey(tableName))) {
      return null;
    }
    List<ServerToSegmentSetMap> serverToSegmentSetMaps = _brokerRoutingTable.get(tableName);

    // This map can be potentially empty, for example for realtime table with no segments.
    if (serverToSegmentSetMaps.isEmpty()) {
      return Collections.emptyMap();
    }
    return serverToSegmentSetMaps.get(_random.nextInt(serverToSegmentSetMaps.size())).getRouting();
  }

  @Override
  public void start() {
    LOGGER.info("Start HelixExternalViewBasedRouting!");

  }

  @Override
  public void shutdown() {
    LOGGER.info("Shutdown HelixExternalViewBasedRouting!");
  }

  public void markDataResourceOnline(String tableName, ExternalView externalView,
      List<InstanceConfig> instanceConfigList) {
    if (externalView == null) {
      return;
    }
    int externalViewRecordVersion = externalView.getRecord().getVersion();
    if (_routingTableLastKnownZkVersionMap.containsKey(tableName)) {
      long lastKnownZkVersion = _routingTableLastKnownZkVersionMap.get(tableName);
      if (externalViewRecordVersion == lastKnownZkVersion) {
        LOGGER.info(
            "No change on routing table version (current version {}, last known version {}), do nothing for table {}",
            externalViewRecordVersion, lastKnownZkVersion, tableName);
        return;
      }

      LOGGER.info(
          "Updating routing table for table {} due to ZK change (current version {}, last known version {})",
          tableName, externalViewRecordVersion, lastKnownZkVersion);
    }

    _routingTableLastKnownZkVersionMap.put(tableName, externalViewRecordVersion);
    if (!_dataTableSet.contains(tableName)) {
      LOGGER.info("Adding a new data table to broker : " + tableName);
      _dataTableSet.add(tableName);
    }
    RoutingTableBuilder routingTableBuilder;
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType != null) {
      switch (tableType) {
        case REALTIME:
          routingTableBuilder = _defaultRealtimeRoutingTableBuilder;
          break;
        case OFFLINE:
          routingTableBuilder = _defaultOfflineRoutingTableBuilder;
          break;
        default:
          routingTableBuilder = _defaultOfflineRoutingTableBuilder;
          break;
      }
    } else {
      routingTableBuilder = _defaultOfflineRoutingTableBuilder;
    }
    if (_routingTableBuilderMap.containsKey(tableName) && (_routingTableBuilderMap.get(tableName) != null)) {
      routingTableBuilder = _routingTableBuilderMap.get(tableName);
    }
    LOGGER.info("Trying to compute routing table for table : " + tableName + ",by : " + routingTableBuilder);
    try {
      List<ServerToSegmentSetMap> serverToSegmentSetMap =
          routingTableBuilder.computeRoutingTableFromExternalView(tableName, externalView, instanceConfigList);

      _brokerRoutingTable.put(tableName, serverToSegmentSetMap);
    } catch (Exception e) {
      LOGGER.error("Failed to compute/update the routing table" + e.getCause(), e);
    }
    try {
      LOGGER.info("Trying to compute time boundary service for table : " + tableName);
      _timeBoundaryService.updateTimeBoundaryService(externalView);
    } catch (Exception e) {
      LOGGER.error("Failed to update the TimeBoundaryService : " + e.getCause(), e);
    }

  }

  public void markDataResourceOffline(String tableName) {
    LOGGER.info("Trying to remove data table from broker : " + tableName);
    if (_dataTableSet.contains(tableName)) {
      _dataTableSet.remove(tableName);
      _brokerRoutingTable.remove(tableName);
      _routingTableLastKnownZkVersionMap.remove(tableName);
      _timeBoundaryService.remove(tableName);
    }
  }

  public boolean contains(String tableName) {
    return _dataTableSet.contains(tableName);
  }

  public Set<String> getDataTableSet() {
    return _dataTableSet;
  }

  public Map<String, List<ServerToSegmentSetMap>> getBrokerRoutingTable() {
    return _brokerRoutingTable;
  }

  public TimeBoundaryService getTimeBoundaryService() {
    return _timeBoundaryService;
  }

  @Override
  public String dumpSnapshot(String tableName) throws Exception {
    JSONObject ret = new JSONObject();
    JSONArray routingTableSnapshot = new JSONArray();

    for (String currentTable : _brokerRoutingTable.keySet()) {
      if (tableName == null || currentTable.startsWith(tableName)) {
        JSONObject tableEntry = new JSONObject();
        tableEntry.put("tableName", currentTable);

        JSONArray entries = new JSONArray();
        List<ServerToSegmentSetMap> routableTable = _brokerRoutingTable.get(currentTable);
        for (ServerToSegmentSetMap serverToInstaceMap : routableTable) {
          entries.put(new JSONObject(serverToInstaceMap.toString()));
        }
        tableEntry.put("routingTableEntries", entries);

        routingTableSnapshot.put(tableEntry);
      }
    }

    ret.put("routingTableSnapshot", routingTableSnapshot);
    ret.put("host", NetUtil.getHostnameOrAddress());
    return ret.toString(2);
  }
}
