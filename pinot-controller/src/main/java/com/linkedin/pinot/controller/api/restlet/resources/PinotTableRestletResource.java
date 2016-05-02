package com.linkedin.pinot.controller.api.restlet.resources;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.controller.api.ControllerRestApplication;
import org.apache.commons.io.FileUtils;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonMappingException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Delete;
import org.restlet.resource.Get;
import org.restlet.resource.Post;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.common.restlet.swagger.HttpVerb;
import com.linkedin.pinot.common.restlet.swagger.Parameter;
import com.linkedin.pinot.common.restlet.swagger.Paths;
import com.linkedin.pinot.common.restlet.swagger.Summary;
import com.linkedin.pinot.common.restlet.swagger.Tags;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse;


public class PinotTableRestletResource extends BasePinotControllerRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableRestletResource.class);
  private final File baseDataDir;
  private final File tempDir;

  public PinotTableRestletResource() throws IOException {
    baseDataDir = new File(_controllerConf.getDataDir());
    if (!baseDataDir.exists()) {
      FileUtils.forceMkdir(baseDataDir);
    }
    tempDir = new File(baseDataDir, "schemasTemp");
    if (!tempDir.exists()) {
      FileUtils.forceMkdir(tempDir);
    }
  }

  @Override
  @Post("json")
  public Representation post(Representation entity) {
    AbstractTableConfig config = null;
    try {
      String jsonRequest = entity.getText();
      config = AbstractTableConfig.init(jsonRequest);
      try {
        addTable(config);
      } catch (Exception e) {
        LOGGER.error("Caught exception while adding table", e);
        ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_ADD_ERROR, 1L);
        setStatus(Status.SERVER_ERROR_INTERNAL);
        return new StringRepresentation("Failed: " + e.getMessage());
      }
      return new StringRepresentation("Success");
    } catch (Exception e) {
      LOGGER.error("error reading/serializing requestJSON", e);
      setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
      return new StringRepresentation("Failed: " + e.getMessage());
    }
  }

  @HttpVerb("post")
  @Summary("Adds a table")
  @Tags({ "table" })
  @Paths({ "/tables", "/tables/" })
  private void addTable(AbstractTableConfig config) throws IOException {
    _pinotHelixResourceManager.addTable(config);
  }

  /**
   * URI Mappings:
   * - "/tables", "/tables/": List all the tables
   * - "/tables/{tableName}", "/tables/{tableName}/": List config for specified table.
   *
   * - "/tables/{tableName}?state={state}"
   *   Set the state for the specified {tableName} to the specified {state} (enable|disable|drop).
   *
   * - "/tables/{tableName}?type={type}"
   *   List all tables of specified type, type can be one of {offline|realtime}.
   *
   *   Set the state for the specified {tableName} to the specified {state} (enable|disable|drop).
   *   * - "/tables/{tableName}?state={state}&amp;type={type}"
   *
   *   Set the state for the specified {tableName} of specified type to the specified {state} (enable|disable|drop).
   *   Type here is type of the table, one of 'offline|realtime'.
   * {@inheritDoc}
   * @see org.restlet.resource.ServerResource#get()
   */
  @Override
  @Get
  public Representation get() {
    final String tableName = (String) getRequest().getAttributes().get(TABLE_NAME);
    final String state = getReference().getQueryAsForm().getValues(STATE);
    final String tableType = getReference().getQueryAsForm().getValues(TABLE_TYPE);

    if (tableType != null && !isValidTableType(tableType)) {
      LOGGER.error(INVALID_TABLE_TYPE_ERROR);
      setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
      return new StringRepresentation(INVALID_TABLE_TYPE_ERROR);
    }

    if (tableName == null) {
      try {
        return getAllTables();
      } catch (Exception e) {
        LOGGER.error("Caught exception while fetching table ", e);
        ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_GET_ERROR, 1L);
        setStatus(Status.SERVER_ERROR_INTERNAL);
        return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
      }
    }
    try {
      if (state == null) {
        return getTable(tableName, tableType);
      } else if (isValidState(state)) {
        return setTablestate(tableName, tableType, state);
      } else {
        LOGGER.error(INVALID_STATE_ERROR);
        setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
        return new StringRepresentation(INVALID_STATE_ERROR);
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while fetching table ", e);
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_GET_ERROR, 1L);
      setStatus(Status.SERVER_ERROR_INTERNAL);
      return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
    }
  }

  @HttpVerb("get")
  @Summary("Views a table's configuration")
  @Tags({ "table" })
  @Paths({ "/tables/{tableName}", "/tables/{tableName}/" })
  private Representation getTable(
      @Parameter(name = "tableName", in = "path", description = "The name of the table for which to toggle its state",
          required = true) String tableName,
      @Parameter(name = "type", in = "query", description = "Type of table, Offline or Realtime", required = true) String tableType)
      throws JSONException, JsonParseException, JsonMappingException, JsonProcessingException, IOException {
    JSONObject ret = new JSONObject();

    if ((tableType == null || TableType.OFFLINE.name().equalsIgnoreCase(tableType))
        && _pinotHelixResourceManager.hasOfflineTable(tableName)) {
      AbstractTableConfig config = _pinotHelixResourceManager.getTableConfig(tableName, TableType.OFFLINE);
      ret.put(TableType.OFFLINE.name(), config.toJSON());
    }

    if ((tableType == null || TableType.REALTIME.name().equalsIgnoreCase(tableType))
        && _pinotHelixResourceManager.hasRealtimeTable(tableName)) {
      AbstractTableConfig config = _pinotHelixResourceManager.getTableConfig(tableName, TableType.REALTIME);
      ret.put(TableType.REALTIME.name(), config.toJSON());
    }

    return new StringRepresentation(ret.toString());
  }

  @HttpVerb("get")
  @Summary("Views all tables' configuration")
  @Tags({ "table" })
  @Paths({ "/tables", "/tables/" })
  private Representation getAllTables() throws JSONException {
    JSONObject object = new JSONObject();
    JSONArray tableArray = new JSONArray();
    Set<String> tableNames = new TreeSet<String>();
    tableNames.addAll(_pinotHelixResourceManager.getAllUniquePinotRawTableNames());
    for (String pinotTableName : tableNames) {
      tableArray.put(pinotTableName);
    }
    object.put("tables", tableArray);
    return new StringRepresentation(object.toString());
  }

  @HttpVerb("get")
  @Summary("Enable, disable or drop a table")
  @Tags({ "table" })
  @Paths({ "/tables/{tableName}", "/table/{tableName}/" })
  private StringRepresentation setTablestate(
      @Parameter(name = "tableName", in = "path", description = "The name of the table for which to toggle its state",
          required = true) String tableName,
      @Parameter(name = "type", in = "query", description = "Type of table, Offline or Realtime", required = false) String type,
      @Parameter(name = "state", in = "query", description = "The desired table state, either enable or disable",
          required = true) String state) throws JSONException {

    JSONArray ret = new JSONArray();
    boolean tableExists = false;

    if ((type == null || TableType.OFFLINE.name().equalsIgnoreCase(type))
        && _pinotHelixResourceManager.hasOfflineTable(tableName)) {
      String offlineTableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(tableName);
      JSONObject offline = new JSONObject();
      tableExists = true;

      offline.put(TABLE_NAME, offlineTableName);
      offline.put(STATE, toggleTableState(offlineTableName, state).toJSON().toString());
      ret.put(offline);
    }

    if ((type == null || TableType.REALTIME.name().equalsIgnoreCase(type))
        && _pinotHelixResourceManager.hasRealtimeTable(tableName)) {
      String realTimeTableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(tableName);
      JSONObject realTime = new JSONObject();
      tableExists = true;

      realTime.put(TABLE_NAME, realTimeTableName);
      realTime.put(STATE, toggleTableState(realTimeTableName, state).toJSON().toString());
      ret.put(realTime);
    }

    return (tableExists) ? new StringRepresentation(ret.toString()) : new StringRepresentation("Error: Table "
        + tableName + " not found.");
  }

  /**
   * Set the state of the specified table to the specified value.
   *
   * @param tableName: Name of table for which to set the state.
   * @param state: One of [enable|disable|drop].
   * @return
   */
  private PinotResourceManagerResponse toggleTableState(String tableName, String state) {
    if (StateType.ENABLE.name().equalsIgnoreCase(state)) {
      return _pinotHelixResourceManager.toggleTableState(tableName, true);
    } else if (StateType.DISABLE.name().equalsIgnoreCase(state)) {
      return _pinotHelixResourceManager.toggleTableState(tableName, false);
    } else if (StateType.DROP.name().equalsIgnoreCase(state)) {
      return _pinotHelixResourceManager.dropTable(tableName);
    } else {
      LOGGER.error(INVALID_STATE_ERROR);
      setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
      return new PinotResourceManagerResponse(INVALID_STATE_ERROR, false);
    }
  }

  @Override
  @Delete
  public Representation delete() {
    StringRepresentation presentation = null;

    final String tableName = (String) getRequest().getAttributes().get(TABLE_NAME);
    final String type = getReference().getQueryAsForm().getValues(TABLE_TYPE);
    if (!deleteTable(tableName, type)) {
      String error = new String("Error: Table " + tableName + " not found.");
      LOGGER.error(error);
      setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
      return new StringRepresentation(error);
    }
    return presentation;
  }

  @HttpVerb("delete")
  @Summary("Deletes a table")
  @Tags({ "table" })
  @Paths({ "/tables/{tableName}", "/tables/{tableName}/" })
  private boolean deleteTable(@Parameter(name = "tableName", in = "path",
      description = "The name of the table to delete", required = true) String tableName, @Parameter(name = "type",
      in = "query", description = "The type of table to delete, either offline or realtime") String type) {
    if (tableName == null) {
      LOGGER.error("Error: Table name null.");
      setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
      return false;
    }
    if (type == null || type.equalsIgnoreCase(TableType.OFFLINE.name())) {
      _pinotHelixResourceManager.deleteOfflineTable(tableName);
    }
    if (type == null || type.equalsIgnoreCase(TableType.REALTIME.name())) {
      _pinotHelixResourceManager.deleteRealtimeTable(tableName);
    }
    return true;
  }
}
