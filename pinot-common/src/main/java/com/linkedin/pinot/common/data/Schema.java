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
package com.linkedin.pinot.common.data;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.helix.ZNRecord;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import static com.linkedin.pinot.common.utils.EqualityUtils.hashCodeOf;
import static com.linkedin.pinot.common.utils.EqualityUtils.isEqual;
import static com.linkedin.pinot.common.utils.EqualityUtils.isNullOrNotSameClass;
import static com.linkedin.pinot.common.utils.EqualityUtils.isSameReference;

/**
 * Schema is defined for each column. To describe the details information of columns.
 * Three types of information are provided.
 * 1. the data type of this column: int, long, double...
 * 2. if this column is a single value column or a multi-value column.
 * 3. the real world business logic: dimensions, metrics and timeStamps.
 * Different indexing and query strategies are used for different data schema types.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Schema {
  private static final Logger LOGGER = LoggerFactory.getLogger(Schema.class);
  private List<MetricFieldSpec> metricFieldSpecs;
  private List<DimensionFieldSpec> dimensionFieldSpecs;
  private TimeFieldSpec timeFieldSpec;
  private String schemaName;

  @JsonIgnore(true)
  private Map<String, FieldSpec> fieldSpecMap;

  @JsonIgnore(true)
  private List<String> dimensions;

  @JsonIgnore(true)
  private List<String> metrics;

  @JsonIgnore(true)
  private String jsonSchema;

  public static Schema fromFile(final File schemaFile)
      throws JsonParseException, JsonMappingException, IOException {
    JsonNode node = new ObjectMapper().readTree(new FileInputStream(schemaFile));
    Schema schema = new ObjectMapper().readValue(node, Schema.class);
    schema.setJSONSchema(node.toString());
    return schema;
  }

  public static Schema fromZNRecord(ZNRecord record)
      throws JsonParseException, JsonMappingException, IOException {
    String schemaJSON = record.getSimpleField("schemaJSON");
    Schema schema = new ObjectMapper().readValue(record.getSimpleField("schemaJSON"), Schema.class);
    schema.setJSONSchema(schemaJSON);
    return schema;
  }

  public static ZNRecord toZNRecord(Schema schema)
      throws IllegalArgumentException, IllegalAccessException {
    ZNRecord record = new ZNRecord(schema.getSchemaName());
    record.setSimpleField("schemaJSON", schema.getJSONSchema());
    return record;
  }

  public Schema() {
    dimensions = new ArrayList<String>();
    metrics = new ArrayList<String>();
    dimensionFieldSpecs = new ArrayList<DimensionFieldSpec>();
    metricFieldSpecs = new ArrayList<MetricFieldSpec>();
    timeFieldSpec = null;
    fieldSpecMap = new HashMap<>();
  }

  public List<MetricFieldSpec> getMetricFieldSpecs() {
    return metricFieldSpecs;
  }

  public void setMetricFieldSpecs(List<MetricFieldSpec> metricFieldSpecs) {
    for (MetricFieldSpec spec : metricFieldSpecs) {
      addField(spec.getName(), spec);
    }
  }

  public List<DimensionFieldSpec> getDimensionFieldSpecs() {
    return dimensionFieldSpecs;
  }

  public void setDimensionFieldSpecs(List<DimensionFieldSpec> dimensionFieldSpecs) {
    for (DimensionFieldSpec spec : dimensionFieldSpecs) {
      addField(spec.getName(), spec);
    }
  }

  public void setTimeFieldSpec(TimeFieldSpec timeFieldSpec) {
    this.timeFieldSpec = timeFieldSpec;
    addField(timeFieldSpec.getName(), timeFieldSpec);
  }

  @JsonIgnore(true)
  public void setJSONSchema(String schemaJSON) {
    jsonSchema = schemaJSON;
  }

  @JsonIgnore(true)
  public String getJSONSchema() {
    return jsonSchema;
  }

  @JsonIgnore(true)
  @Deprecated // use addField
  public void addSchema(String columnName, FieldSpec fieldSpec) {
    addField(columnName, fieldSpec);
  }

  @JsonIgnore(true)
  public void addField(String columnName, FieldSpec fieldSpec) {
    if (fieldSpec.getName() == null) {
      fieldSpec.setName(columnName);
    }

    if (columnName != null) {
      fieldSpecMap.put(columnName, fieldSpec);
      if (fieldSpec.getFieldType() == FieldType.DIMENSION) {
        if (!dimensions.contains(columnName)) {
          dimensions.add(columnName);
          dimensionFieldSpecs.add((DimensionFieldSpec) fieldSpec);
        }
      } else if (fieldSpec.getFieldType() == FieldType.METRIC) {
        if (!metrics.contains(columnName)) {
          metrics.add(columnName);
          metricFieldSpecs.add((MetricFieldSpec) fieldSpec);
        }
      } else if (fieldSpec.getFieldType() == FieldType.TIME) {
        timeFieldSpec = (TimeFieldSpec) fieldSpec;
      }
    }
  }

  @JsonIgnore(true)
  public boolean hasColumn(String columnName) {
    if (dimensions.contains(columnName)) {
      return true;
    }
    if (metrics.contains(columnName)) {
      return true;
    }
    if (timeFieldSpec != null && timeFieldSpec.getName().equals(columnName)) {
      return true;
    }
    return false;
  }

  @JsonIgnore(true)
  public Collection<String> getColumnNames() {
    Set<String> ret = new HashSet<String>();
    ret.addAll(metrics);
    ret.addAll(dimensions);
    if (timeFieldSpec != null) {
      ret.add(timeFieldSpec.getName());
    }
    return ret;
  }

  @JsonIgnore(true)
  public int size() {
    return metricFieldSpecs.size() + dimensionFieldSpecs.size() + (timeFieldSpec == null ? 0 : 1);
  }

  @JsonIgnore(true)
  public FieldSpec getFieldSpecFor(String column) {
    return fieldSpecMap.get(column);
  }

  @JsonIgnore(true)
  public MetricFieldSpec getMetricSpec(final String metricName) {
    FieldSpec fieldSpec = fieldSpecMap.get(metricName);
    if (fieldSpec != null && fieldSpec.getFieldType() == FieldType.METRIC) {
      return (MetricFieldSpec) fieldSpec;
    }
    return null;
  }

  @JsonIgnore(true)
  public DimensionFieldSpec getDimensionSpec(final String dimensionName) {
    FieldSpec fieldSpec = fieldSpecMap.get(dimensionName);
    if (fieldSpec != null && fieldSpec.getFieldType() == FieldType.DIMENSION) {
      return (DimensionFieldSpec) fieldSpec;
    }
    return null;
  }

  @JsonIgnore(true)
  public Collection<FieldSpec> getAllFieldSpecs() {
    return fieldSpecMap.values();
  }

  @JsonIgnore(true)
  public List<String> getDimensionNames() {
    return new ArrayList<String>(dimensions);
  }

  @JsonIgnore(true)
  public List<String> getMetricNames() {
    return new ArrayList<String>(metrics);
  }

  @JsonIgnore(true)
  public String getTimeColumnName() {
    return (timeFieldSpec != null) ? timeFieldSpec.getName() : null;
  }

  @JsonIgnore(true)
  public TimeUnit getIncomingTimeUnit() {
    return (timeFieldSpec != null && timeFieldSpec.getIncomingGranularitySpec() != null)
        ? timeFieldSpec.getIncomingGranularitySpec().getTimeType() : null;
  }

  @JsonIgnore(true)
  public TimeUnit getOutgoingTimeUnit() {
    return (timeFieldSpec != null && timeFieldSpec.getOutgoingGranularitySpec() != null)
        ? timeFieldSpec.getIncomingGranularitySpec().getTimeType() : null;
  }

  public TimeFieldSpec getTimeFieldSpec() {
    return timeFieldSpec;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  /**
   * Validates a pinot schema. The following validations are performed:
   * - All fields must have a default value.
   * @param ctxLogger to log the message (if null, the current class logger is used)
   * @return
   */
  public boolean validate(Logger ctxLogger) {
    Collection<FieldSpec> fieldSpecs = getAllFieldSpecs();
    if (ctxLogger == null) {
      ctxLogger = LOGGER;
    }
    boolean isValid = true;
    // Log ALL the schema errors that may be present.
    for (FieldSpec fieldSpec : fieldSpecs) {
      Object o = null;
      try {
        if (fieldSpec.getDataType().equals(DataType.BOOLEAN) && fieldSpec.getFieldType().equals(FieldType.METRIC)) {
          ctxLogger.error("Boolean field {} cannot be a metric ", fieldSpec.getName());
          isValid = false;
          continue;
        }
        o = fieldSpec.getDefaultNullValue();
      } catch (Exception e) {
      }
      if (o == null) {
        if (fieldSpec.getDataType().equals(DataType.BOOLEAN)) {
          ctxLogger.warn("Ignoring field {} of type {} does not have a default value", fieldSpec.getName(),
              fieldSpec.getFieldType());
        } else {
          ctxLogger.error("Field {} of type {} does not have a default value", fieldSpec.getName(), fieldSpec.getFieldType());
          isValid = false;
        }
      }
    }
    return isValid;
  }

  @Override
  public String toString() {
    JSONObject ret = new JSONObject();
    try {
      ret.put("metricFieldSpecs", new ObjectMapper().writeValueAsString(metricFieldSpecs));
      ret.put("dimensionFieldSpecs", new ObjectMapper().writeValueAsString(dimensionFieldSpecs));
      if (timeFieldSpec != null) {
        JSONObject time = new JSONObject();
        time.put("incomingGranularitySpec",
            new ObjectMapper().writeValueAsString(timeFieldSpec.getIncomingGranularitySpec()));
        time.put("outgoingGranularitySpec",
            new ObjectMapper().writeValueAsString(timeFieldSpec.getOutgoingGranularitySpec()));
        ret.put("timeFieldSpec", time);
      } else {
        ret.put("timeFieldSpec", new JSONObject());
      }
      ret.put("schemaName", schemaName);
    } catch (Exception e) {
      LOGGER.error("error processing toString on Schema : ", this.schemaName, e);
      return null;
    }
    return ret.toString();
  }

  public static class SchemaBuilder {
    private Schema schema;

    public SchemaBuilder() {
      schema = new Schema();
    }

    public SchemaBuilder setSchemaName(String schemaName) {
      schema.setSchemaName(schemaName);
      return this;
    }

    public SchemaBuilder addSingleValueDimension(String dimensionName, DataType type) {
      FieldSpec spec = new DimensionFieldSpec();
      spec.setSingleValueField(true);
      spec.setDataType(type);
      spec.setName(dimensionName);
      schema.addField(dimensionName, spec);
      return this;
    }

    public SchemaBuilder addMultiValueDimension(String dimensionName, DataType dataType,
        String delimiter) {
      FieldSpec spec = new DimensionFieldSpec();
      spec.setSingleValueField(false);
      spec.setDataType(dataType);
      spec.setName(dimensionName);
      spec.setDelimiter(delimiter);

      schema.addField(dimensionName, spec);
      return this;
    }

    public SchemaBuilder addMetric(String metricName, DataType dataType) {
      FieldSpec spec = new MetricFieldSpec();
      spec.setSingleValueField(true);
      spec.setDataType(dataType);
      spec.setName(metricName);

      schema.addField(metricName, spec);
      return this;
    }

    public SchemaBuilder addTime(String incomingColumnName, TimeUnit incomingGranularity,
        DataType incomingDataType) {
      TimeGranularitySpec incomingGranularitySpec =
          new TimeGranularitySpec(incomingDataType, incomingGranularity, incomingColumnName);
      addTime(incomingColumnName, incomingGranularitySpec);
      return this;
    }

    public SchemaBuilder addTime(String incomingColumnName, TimeUnit incomingGranularity,
        DataType incomingDataType, String outGoingColumnName, TimeUnit outgoingGranularity,
        DataType outgoingDataType) {
      TimeGranularitySpec incoming =
          new TimeGranularitySpec(incomingDataType, incomingGranularity, incomingColumnName);
      TimeGranularitySpec outgoing =
          new TimeGranularitySpec(outgoingDataType, outgoingGranularity, outGoingColumnName);
      addTime(incomingColumnName, incoming, outgoing);
      return this;
    }

    public SchemaBuilder addTime(String incomingColumnName, int incomingSize,
        TimeUnit incomingGranularity, DataType incomingDataType) {
      TimeGranularitySpec incomingGranularitySpec = new TimeGranularitySpec(incomingDataType,
          incomingSize, incomingGranularity, incomingColumnName);
      addTime(incomingColumnName, incomingGranularitySpec);
      return this;
    }

    public SchemaBuilder addTime(String incomingColumnName, int incomingSize,
        TimeUnit incomingGranularity, DataType incomingDataType, String outGoingColumnName,
        int outgoingSize, TimeUnit outgoingGranularity, DataType outgoingDataType) {
      TimeGranularitySpec incoming = new TimeGranularitySpec(incomingDataType, incomingSize,
          incomingGranularity, incomingColumnName);
      TimeGranularitySpec outgoing = new TimeGranularitySpec(outgoingDataType, outgoingSize,
          outgoingGranularity, outGoingColumnName);
      addTime(incomingColumnName, incoming, outgoing);
      return this;
    }

    public void addTime(String incomingColumnName, TimeGranularitySpec incoming,
        TimeGranularitySpec outgoing) {
      schema.addField(incomingColumnName, new TimeFieldSpec(incoming, outgoing));
    }

    public void addTime(String incomingColumnName, TimeGranularitySpec incoming) {
      schema.addField(incomingColumnName, new TimeFieldSpec(incoming));
    }

    public Schema build() {
      return schema;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (isSameReference(this, o)) {
      return true;
    }

    if (isNullOrNotSameClass(this, o)) {
      return false;
    }

    Schema other = (Schema) o;

    return isEqual(dimensions, other.dimensions) && isEqual(timeFieldSpec, other.timeFieldSpec)
        && isEqual(metrics, other.metrics) && isEqual(schemaName, other.schemaName)
        && isEqual(metricFieldSpecs, other.metricFieldSpecs)
        && isEqual(dimensionFieldSpecs, other.dimensionFieldSpecs);
  }

  @Override
  public int hashCode() {
    int result = hashCodeOf(dimensionFieldSpecs);
    result = hashCodeOf(result, metricFieldSpecs);
    result = hashCodeOf(result, timeFieldSpec);
    result = hashCodeOf(result, dimensions);
    result = hashCodeOf(result, metrics);
    result = hashCodeOf(result, schemaName);
    return result;
  }
}
