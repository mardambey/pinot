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
package com.linkedin.pinot.core.query.selection;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.request.SelectionSort;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.response.broker.SelectionResults;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.common.utils.DataTableBuilder;
import com.linkedin.pinot.common.utils.DataTableBuilder.DataSchema;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockMultiValIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.blocks.MultiValueBlock;
import com.linkedin.pinot.core.operator.blocks.RealtimeMultiValueBlock;
import com.linkedin.pinot.core.operator.blocks.RealtimeSingleValueBlock;
import com.linkedin.pinot.core.operator.blocks.SortedSingleValueBlock;
import com.linkedin.pinot.core.operator.blocks.UnSortedSingleValueBlock;
import com.linkedin.pinot.core.realtime.impl.dictionary.DoubleMutableDictionary;
import com.linkedin.pinot.core.realtime.impl.dictionary.FloatMutableDictionary;
import com.linkedin.pinot.core.realtime.impl.dictionary.IntMutableDictionary;
import com.linkedin.pinot.core.realtime.impl.dictionary.LongMutableDictionary;
import com.linkedin.pinot.core.realtime.impl.dictionary.StringMutableDictionary;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.segment.index.readers.DoubleDictionary;
import com.linkedin.pinot.core.segment.index.readers.FloatDictionary;
import com.linkedin.pinot.core.segment.index.readers.IntDictionary;
import com.linkedin.pinot.core.segment.index.readers.LongDictionary;
import com.linkedin.pinot.core.segment.index.readers.StringDictionary;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * SelectionOperator provides the apis for selection query.
 *
 *
 */
public class SelectionOperatorUtils {

  public static final Map<DataType, DecimalFormat> DEFAULT_FORMAT_STRING_MAP = new HashMap<DataType, DecimalFormat>();

  static {
    DEFAULT_FORMAT_STRING_MAP
        .put(DataType.INT, new DecimalFormat("##########", DecimalFormatSymbols.getInstance(Locale.US)));
    DEFAULT_FORMAT_STRING_MAP
        .put(DataType.LONG, new DecimalFormat("####################", DecimalFormatSymbols.getInstance(Locale.US)));
    DEFAULT_FORMAT_STRING_MAP
        .put(DataType.FLOAT, new DecimalFormat("##########.#####", DecimalFormatSymbols.getInstance(Locale.US)));
    DEFAULT_FORMAT_STRING_MAP.put(DataType.DOUBLE,
        new DecimalFormat("####################.##########", DecimalFormatSymbols.getInstance(Locale.US)));
    DEFAULT_FORMAT_STRING_MAP
        .put(DataType.INT_ARRAY, new DecimalFormat("##########", DecimalFormatSymbols.getInstance(Locale.US)));
    DEFAULT_FORMAT_STRING_MAP.put(DataType.LONG_ARRAY,
        new DecimalFormat("####################", DecimalFormatSymbols.getInstance(Locale.US)));
    DEFAULT_FORMAT_STRING_MAP
        .put(DataType.FLOAT_ARRAY, new DecimalFormat("##########.#####", DecimalFormatSymbols.getInstance(Locale.US)));
    DEFAULT_FORMAT_STRING_MAP.put(DataType.DOUBLE_ARRAY,
        new DecimalFormat("####################.##########", DecimalFormatSymbols.getInstance(Locale.US)));
  }

  public static List<String> getSelectionColumns(List<String> selectionColumns, IndexSegment indexSegment) {
    if ((selectionColumns.size() == 1) && selectionColumns.get(0).equals("*")) {
      selectionColumns.clear();
      for (final String columnName : indexSegment.getColumnNames()) {
        selectionColumns.add(columnName);
      }
    }
    return selectionColumns;
  }

  public static List<String> getSelectionColumns(List<String> selectionColumns, DataSchema dataSchema) {
    if ((selectionColumns.size() == 1) && selectionColumns.get(0).equals("*")) {
      selectionColumns.clear();
      for (int i = 0; i < dataSchema.size(); ++i) {
        selectionColumns.add(dataSchema.getColumnName(i));
      }
    }
    return selectionColumns;
  }

  public static String[] extractSelectionRelatedColumns(Selection selection, IndexSegment indexSegment) {
    Set<String> selectionColumns = new HashSet<String>();
    selectionColumns.addAll(selection.getSelectionColumns());
    if ((selectionColumns.size() == 1) && ((selectionColumns.toArray(new String[0]))[0].equals("*"))) {
      selectionColumns.clear();
      selectionColumns.addAll(Arrays.asList(indexSegment.getColumnNames()));
    }
    if (selection.getSelectionSortSequence() != null) {
      for (SelectionSort selectionSort : selection.getSelectionSortSequence()) {
        selectionColumns.add(selectionSort.getColumn());
      }
    }
    return selectionColumns.toArray(new String[0]);
  }

  public static Collection<Serializable[]> merge(Collection<Serializable[]> rowEventsSet1,
      Collection<Serializable[]> rowEventsSet2, int maxRowSize) {
    if (rowEventsSet1 == null) {
      return rowEventsSet2;
    }
    if (rowEventsSet2 == null) {
      return rowEventsSet1;
    }
    final Iterator<Serializable[]> iterator = rowEventsSet2.iterator();
    while (rowEventsSet1.size() < maxRowSize && iterator.hasNext()) {
      final Serializable[] row = iterator.next();
      rowEventsSet1.add(row);
    }
    return rowEventsSet1;
  }

  public static Collection<Serializable[]> reduce(Map<ServerInstance, DataTable> selectionResults, int maxRowSize) {
    Collection<Serializable[]> rowEventsSet = new ArrayList<Serializable[]>(maxRowSize);
    for (final DataTable dt : selectionResults.values()) {
      for (int rowId = 0; rowId < dt.getNumberOfRows(); ++rowId) {
        final Serializable[] row = extractRowFromDataTable(dt, rowId);
        rowEventsSet.add(row);
        if (rowEventsSet.size() == maxRowSize) {
          return rowEventsSet;
        }
      }
    }
    return rowEventsSet;
  }

  public static JSONObject render(Collection<Serializable[]> finalResults, List<String> selectionColumns,
      DataSchema dataSchema)
      throws Exception {
    final LinkedList<JSONArray> rowEventsJSonList = new LinkedList<JSONArray>();
    List<Serializable[]> list = (List<Serializable[]>) finalResults;
    if (selectionColumns.size() == 1 && selectionColumns.get(0).equals("*")) {
      selectionColumns = getSelectionColumns(selectionColumns, dataSchema);
    }
    for (int i = 0; i < list.size(); i++) {
      rowEventsJSonList.add(getJSonArrayFromRow(list.get(i), selectionColumns, dataSchema));
    }
    final JSONObject resultJsonObject = new JSONObject();
    resultJsonObject.put("results", new JSONArray(rowEventsJSonList));
    resultJsonObject.put("columns", getSelectionColumnsFromDataSchema(selectionColumns, dataSchema));
    return resultJsonObject;
  }

  /**
   * Given a collection of selection results from broker, format and render the SelectionResults
   * object, to be used in building the BrokerResponse.
   *
   * @param reducedResults
   * @param selectionColumns
   * @param dataSchema
   * @return
   * @throws Exception
   */
  public static SelectionResults renderSelectionResults(Collection<Serializable[]> reducedResults,
      List<String> selectionColumns, DataSchema dataSchema)
      throws Exception {
    List<Serializable[]> rowData = (List<Serializable[]>) reducedResults;
    List<Serializable[]> rows = new ArrayList<Serializable[]>();

    if (selectionColumns.size() == 1 && selectionColumns.get(0).equals("*")) {
      selectionColumns = getSelectionColumns(selectionColumns, dataSchema);
    }

    for (int i = 0; i < rowData.size(); i++) {
      rows.add(getFormattedRow(rowData.get(i), selectionColumns, dataSchema));
    }

    return new SelectionResults(selectionColumns, rows);
  }

  private static JSONArray getSelectionColumnsFromDataSchema(List<String> selectionColumns, DataSchema dataSchema) {
    final JSONArray jsonArray = new JSONArray();
    for (int idx = 0; idx < dataSchema.size(); ++idx) {
      if (selectionColumns.contains(dataSchema.getColumnName(idx))) {
        jsonArray.put(dataSchema.getColumnName(idx));
      }
    }
    return jsonArray;
  }

  public static DataSchema extractDataSchema(String[] selectionColumns, IndexSegment indexSegment) {
    return extractDataSchema(null, Arrays.asList(selectionColumns), indexSegment);
  }

  public static DataSchema extractDataSchema(List<SelectionSort> sortSequence, List<String> selectionColumns,
      IndexSegment indexSegment) {
    final List<String> columns = new ArrayList<String>();

    if (sortSequence != null && !sortSequence.isEmpty()) {
      for (final SelectionSort selectionSort : sortSequence) {
        columns.add(selectionSort.getColumn());
      }
    }
    String[] selectionColumnArray = selectionColumns.toArray(new String[selectionColumns.size()]);
    Arrays.sort(selectionColumnArray);
    for (int i = 0; i < selectionColumnArray.length; ++i) {
      String selectionColumn = selectionColumnArray[i];
      if (!columns.contains(selectionColumn)) {
        columns.add(selectionColumn);
      }
    }
    final DataType[] dataTypes = new DataType[columns.size()];
    for (int i = 0; i < dataTypes.length; ++i) {
      DataSource ds = indexSegment.getDataSource(columns.get(i));
      DataSourceMetadata m = ds.getDataSourceMetadata();
      dataTypes[i] = m.getDataType();
      if (!m.isSingleValue()) {
        dataTypes[i] = DataType.valueOf(dataTypes[i] + "_ARRAY");
      }
    }
    return new DataSchema(columns.toArray(new String[0]), dataTypes);
  }

  @Deprecated
  public static Serializable[] collectRowFromBlockValSets(int docId, Block[] blocks, DataSchema dataSchema) {

    final Serializable[] row = new Serializable[dataSchema.size()];
    int j = 0;
    for (int i = 0; i < dataSchema.size(); ++i) {
      if (blocks[j] instanceof RealtimeSingleValueBlock) {
        if (blocks[j].getMetadata().hasDictionary()) {
          Dictionary dictionaryReader = blocks[j].getMetadata().getDictionary();
          BlockSingleValIterator bvIter = (BlockSingleValIterator) blocks[j].getBlockValueSet().iterator();
          bvIter.skipTo(docId);
          switch (dataSchema.getColumnType(i)) {
            case INT:
              row[i] = (Integer) ((IntMutableDictionary) dictionaryReader).get(bvIter.nextIntVal());
              break;
            case FLOAT:
              row[i] = (Float) ((FloatMutableDictionary) dictionaryReader).get(bvIter.nextIntVal());
              break;
            case LONG:
              row[i] = (Long) ((LongMutableDictionary) dictionaryReader).get(bvIter.nextIntVal());
              break;
            case DOUBLE:
              row[i] = (Double) ((DoubleMutableDictionary) dictionaryReader).get(bvIter.nextIntVal());
              break;
            case STRING:
              row[i] = (String) ((StringMutableDictionary) dictionaryReader).get(bvIter.nextIntVal());
              break;
            default:
              break;
          }
        } else {
          BlockSingleValIterator bvIter = (BlockSingleValIterator) blocks[j].getBlockValueSet().iterator();
          bvIter.skipTo(docId);
          switch (dataSchema.getColumnType(i)) {
            case INT:
              row[i] = bvIter.nextIntVal();
              break;
            case FLOAT:
              row[i] = bvIter.nextFloatVal();
              break;
            case LONG:
              row[i] = bvIter.nextLongVal();
              break;
            case DOUBLE:
              row[i] = bvIter.nextDoubleVal();
              break;
            default:
              break;
          }
        }
      } else if (blocks[j] instanceof RealtimeMultiValueBlock) {
        Dictionary dictionaryReader = blocks[j].getMetadata().getDictionary();
        BlockMultiValIterator bvIter = (BlockMultiValIterator) blocks[j].getBlockValueSet().iterator();
        bvIter.skipTo(docId);
        int[] dictIds = new int[blocks[j].getMetadata().getMaxNumberOfMultiValues()];
        int dictSize;
        switch (dataSchema.getColumnType(i)) {
          case INT_ARRAY:
            dictSize = bvIter.nextIntVal(dictIds);
            int[] rawIntRow = new int[dictSize];
            for (int dictIdx = 0; dictIdx < dictSize; ++dictIdx) {
              rawIntRow[dictIdx] = (Integer) ((IntMutableDictionary) dictionaryReader).get(dictIds[dictIdx]);
            }
            row[i] = rawIntRow;
            break;
          case FLOAT_ARRAY:
            dictSize = bvIter.nextIntVal(dictIds);
            Float[] rawFloatRow = new Float[dictSize];
            for (int dictIdx = 0; dictIdx < dictSize; ++dictIdx) {
              rawFloatRow[dictIdx] = (Float) ((FloatMutableDictionary) dictionaryReader).get(dictIds[dictIdx]);
            }
            row[i] = rawFloatRow;
            break;
          case LONG_ARRAY:
            dictSize = bvIter.nextIntVal(dictIds);
            Long[] rawLongRow = new Long[dictSize];
            for (int dictIdx = 0; dictIdx < dictSize; ++dictIdx) {
              rawLongRow[dictIdx] = (Long) ((LongMutableDictionary) dictionaryReader).get(dictIds[dictIdx]);
            }
            row[i] = rawLongRow;
            break;
          case DOUBLE_ARRAY:
            dictSize = bvIter.nextIntVal(dictIds);
            Double[] rawDoubleRow = new Double[dictSize];
            for (int dictIdx = 0; dictIdx < dictSize; ++dictIdx) {
              rawDoubleRow[dictIdx] = (Double) ((DoubleMutableDictionary) dictionaryReader).get(dictIds[dictIdx]);
            }
            row[i] = rawDoubleRow;
            break;
          case STRING_ARRAY:
            dictSize = bvIter.nextIntVal(dictIds);
            String[] rawStringRow = new String[dictSize];
            for (int dictIdx = 0; dictIdx < dictSize; ++dictIdx) {
              rawStringRow[dictIdx] = (String) (((StringMutableDictionary) dictionaryReader).get(dictIds[dictIdx]));
            }
            row[i] = rawStringRow;
            break;
          default:
            break;
        }
      } else if (blocks[j] instanceof UnSortedSingleValueBlock || blocks[j] instanceof SortedSingleValueBlock) {
        if (blocks[j].getMetadata().hasDictionary()) {
          Dictionary dictionaryReader = blocks[j].getMetadata().getDictionary();
          BlockSingleValIterator bvIter = (BlockSingleValIterator) blocks[j].getBlockValueSet().iterator();
          bvIter.skipTo(docId);
          switch (dataSchema.getColumnType(i)) {
            case INT:
              int dicId = bvIter.nextIntVal();
              row[i] = ((IntDictionary) dictionaryReader).get(dicId);
              break;
            case FLOAT:
              row[i] = ((FloatDictionary) dictionaryReader).get(bvIter.nextIntVal());
              break;
            case LONG:
              row[i] = ((LongDictionary) dictionaryReader).get(bvIter.nextIntVal());
              break;
            case DOUBLE:
              row[i] = ((DoubleDictionary) dictionaryReader).get(bvIter.nextIntVal());
              break;
            case STRING:
              row[i] = ((StringDictionary) dictionaryReader).get(bvIter.nextIntVal());
              break;
            default:
              break;
          }
        } else {
          BlockSingleValIterator bvIter = (BlockSingleValIterator) blocks[j].getBlockValueSet().iterator();
          bvIter.skipTo(docId);
          switch (dataSchema.getColumnType(i)) {
            case INT:
              row[i] = new Integer(bvIter.nextIntVal());
              break;
            case FLOAT:
              row[i] = new Float(bvIter.nextFloatVal());
              break;
            case LONG:
              row[i] = new Long(bvIter.nextLongVal());
              break;
            case DOUBLE:
              row[i] = new Double(bvIter.nextDoubleVal());
              break;
            default:
              break;
          }
        }
      } else if (blocks[j] instanceof MultiValueBlock) {
        Dictionary dictionaryReader = blocks[j].getMetadata().getDictionary();
        BlockMultiValIterator bvIter = (BlockMultiValIterator) blocks[j].getBlockValueSet().iterator();
        bvIter.skipTo(docId);
        int[] dictIds = new int[blocks[j].getMetadata().getMaxNumberOfMultiValues()];
        int dictSize;
        switch (dataSchema.getColumnType(i)) {
          case INT_ARRAY:
            dictSize = bvIter.nextIntVal(dictIds);
            int[] rawIntRow = new int[dictSize];
            for (int dictIdx = 0; dictIdx < dictSize; ++dictIdx) {
              rawIntRow[dictIdx] = ((IntDictionary) dictionaryReader).get(dictIds[dictIdx]);
            }
            row[i] = rawIntRow;
            break;
          case FLOAT_ARRAY:
            dictSize = bvIter.nextIntVal(dictIds);
            Float[] rawFloatRow = new Float[dictSize];
            for (int dictIdx = 0; dictIdx < dictSize; ++dictIdx) {
              rawFloatRow[dictIdx] = ((FloatDictionary) dictionaryReader).get(dictIds[dictIdx]);
            }
            row[i] = rawFloatRow;
            break;
          case LONG_ARRAY:
            dictSize = bvIter.nextIntVal(dictIds);
            Long[] rawLongRow = new Long[dictSize];
            for (int dictIdx = 0; dictIdx < dictSize; ++dictIdx) {
              rawLongRow[dictIdx] = ((LongDictionary) dictionaryReader).get(dictIds[dictIdx]);
            }
            row[i] = rawLongRow;
            break;
          case DOUBLE_ARRAY:
            dictSize = bvIter.nextIntVal(dictIds);
            Double[] rawDoubleRow = new Double[dictSize];
            for (int dictIdx = 0; dictIdx < dictSize; ++dictIdx) {
              rawDoubleRow[dictIdx] = ((DoubleDictionary) dictionaryReader).get(dictIds[dictIdx]);
            }
            row[i] = rawDoubleRow;
            break;
          case STRING_ARRAY:
            dictSize = bvIter.nextIntVal(dictIds);
            String[] rawStringRow = new String[dictSize];
            for (int dictIdx = 0; dictIdx < dictSize; ++dictIdx) {
              rawStringRow[dictIdx] = (((StringDictionary) dictionaryReader).get(dictIds[dictIdx]));
            }
            row[i] = rawStringRow;
            break;
          default:
            break;
        }
      }
      j++;
    }
    return row;
  }

  public static JSONArray getJSonArrayFromRow(Serializable[] poll, List<String> selectionColumns, DataSchema dataSchema)
      throws JSONException {

    final JSONArray jsonArray = new JSONArray();
    for (int i = 0; i < dataSchema.size(); ++i) {
      if (selectionColumns.contains(dataSchema.getColumnName(i))) {
        if (dataSchema.getColumnType(i).isSingleValue()) {
          if (dataSchema.getColumnType(i) == DataType.STRING) {
            jsonArray.put(poll[i]);
          } else {
            jsonArray.put(DEFAULT_FORMAT_STRING_MAP.get(dataSchema.getColumnType(i)).format(poll[i]));
          }
        } else {
          // Multi-value;

          JSONArray stringJsonArray = new JSONArray();
          //          stringJsonArray.put(poll[i]);
          switch (dataSchema.getColumnType(i)) {
            case STRING_ARRAY:
              String[] stringValues = (String[]) poll[i];
              for (String s : stringValues) {
                stringJsonArray.put(s);
              }
              break;
            case INT_ARRAY:
              int[] intValues = (int[]) poll[i];
              for (int s : intValues) {
                stringJsonArray.put(DEFAULT_FORMAT_STRING_MAP.get(dataSchema.getColumnType(i)).format(s));
              }
              break;
            case FLOAT_ARRAY:
              float[] floatValues = (float[]) poll[i];
              for (float s : floatValues) {
                stringJsonArray.put(DEFAULT_FORMAT_STRING_MAP.get(dataSchema.getColumnType(i)).format(s));
              }
              break;
            case LONG_ARRAY:
              long[] longValues = (long[]) poll[i];
              for (long s : longValues) {
                stringJsonArray.put(DEFAULT_FORMAT_STRING_MAP.get(dataSchema.getColumnType(i)).format(s));
              }
              break;
            case DOUBLE_ARRAY:
              double[] doubleValues = (double[]) poll[i];
              for (double s : doubleValues) {
                stringJsonArray.put(DEFAULT_FORMAT_STRING_MAP.get(dataSchema.getColumnType(i)).format(s));
              }
              break;
            default:
              break;
          }
          jsonArray.put(stringJsonArray);
        }
      }
    }
    return jsonArray;
  }

  /**
   * Given an array of input values, format them as per their data type and return the formatted array.
   * The formatted object in the array are of type String.
   *
   * @param inputValues To be formatted
   * @param selectionColumns Columns to pick up
   * @param dataSchema
   * @return
   * @throws JSONException
   */
  public static Serializable[] getFormattedRow(Serializable[] inputValues, List<String> selectionColumns,
      DataSchema dataSchema)
      throws JSONException {
    Serializable[] formattedRow = new Serializable[inputValues.length];

    for (int i = 0; i < dataSchema.size(); ++i) {
      // If column not part of schema, we don't know how to format it, so just skip.
      if (!selectionColumns.contains(dataSchema.getColumnName(i))) {
        continue;
      }

      if (dataSchema.getColumnType(i).isSingleValue()) {
        if (dataSchema.getColumnType(i) == DataType.STRING) {
          formattedRow[i] = inputValues[i];
        } else {
          formattedRow[i] = DEFAULT_FORMAT_STRING_MAP.get(dataSchema.getColumnType(i)).format(inputValues[i]);
        }
      } else {
        String[] multiValued;
        DecimalFormat decimalFormat = DEFAULT_FORMAT_STRING_MAP.get(dataSchema.getColumnType(i));

        switch (dataSchema.getColumnType(i)) {
          case STRING_ARRAY:
            multiValued = (String[]) inputValues[i];
            break;

          case INT_ARRAY:
            int[] intValues = (int[]) inputValues[i];
            multiValued = new String[intValues.length];
            for (int j = 0; j < intValues.length; j++) {
              multiValued[j] = decimalFormat.format(intValues[j]);
            }
            break;

          case FLOAT_ARRAY:
            float[] floatValues = (float[]) inputValues[i];
            multiValued = new String[floatValues.length];
            for (int j = 0; j < floatValues.length; j++) {
              multiValued[j] = decimalFormat.format(floatValues[j]);
            }
            break;

          case LONG_ARRAY:
            long[] longValues = (long[]) inputValues[i];
            multiValued = new String[longValues.length];
            for (int j = 0; j < longValues.length; j++) {
              multiValued[j] = decimalFormat.format(longValues[j]);
            }
            break;

          case DOUBLE_ARRAY:
            double[] doubleValues = (double[]) inputValues[i];
            multiValued = new String[doubleValues.length];
            for (int j = 0; j < doubleValues.length; j++) {
              multiValued[j] = decimalFormat.format(doubleValues[j]);
            }
            break;

          default:
            throw new RuntimeException("Unsupported data type in selection results");
        }
        formattedRow[i] = multiValued;
      }
    }
    return formattedRow;
  }

  public static Serializable[] extractRowFromDataTable(DataTable dt, int rowId) {
    final Serializable[] row = new Serializable[dt.getDataSchema().size()];
    for (int i = 0; i < dt.getDataSchema().size(); ++i) {
      if (dt.getDataSchema().getColumnType(i).isSingleValue()) {
        switch (dt.getDataSchema().getColumnType(i)) {
          case INT:
            row[i] = dt.getInt(rowId, i);
            break;
          case LONG:
            row[i] = dt.getLong(rowId, i);
            break;
          case DOUBLE:
            row[i] = dt.getDouble(rowId, i);
            break;
          case FLOAT:
            row[i] = dt.getFloat(rowId, i);
            break;
          case STRING:
            row[i] = dt.getString(rowId, i);
            break;
          case SHORT:
            row[i] = dt.getShort(rowId, i);
            break;
          case CHAR:
            row[i] = dt.getChar(rowId, i);
            break;
          case BYTE:
            row[i] = dt.getByte(rowId, i);
            break;
          default:
            row[i] = dt.getObject(rowId, i);
            break;
        }
      } else {
        switch (dt.getDataSchema().getColumnType(i)) {
          case INT_ARRAY:
            row[i] = dt.getIntArray(rowId, i);
            break;
          case LONG_ARRAY:
            row[i] = dt.getLongArray(rowId, i);
            break;
          case DOUBLE_ARRAY:
            row[i] = dt.getDoubleArray(rowId, i);
            break;
          case FLOAT_ARRAY:
            row[i] = dt.getFloatArray(rowId, i);
            break;
          case STRING_ARRAY:
            row[i] = dt.getStringArray(rowId, i);
            break;
          case CHAR_ARRAY:
            row[i] = dt.getCharArray(rowId, i);
            break;
          case BYTE_ARRAY:
            row[i] = dt.getByteArray(rowId, i);
            break;
          default:
            row[i] = dt.getObject(rowId, i);
            break;
        }
      }
    }
    return row;
  }

  public static DataTable getDataTableFromRowSet(Collection<Serializable[]> rowEventsSet, DataSchema dataSchema)
      throws Exception {
    final DataTableBuilder dataTableBuilder = new DataTableBuilder(dataSchema);
    dataTableBuilder.open();
    final Iterator<Serializable[]> iterator = rowEventsSet.iterator();
    while (iterator.hasNext()) {
      final Serializable[] row = iterator.next();
      dataTableBuilder.startRow();
      for (int i = 0; i < dataSchema.size(); ++i) {
        if (dataSchema.getColumnType(i).isSingleValue()) {
          switch (dataSchema.getColumnType(i)) {
            case INT:
              dataTableBuilder.setColumn(i, ((Integer) row[i]).intValue());
              break;
            case LONG:
              dataTableBuilder.setColumn(i, ((Long) row[i]).longValue());
              break;
            case DOUBLE:
              dataTableBuilder.setColumn(i, ((Double) row[i]).doubleValue());
              break;
            case FLOAT:
              dataTableBuilder.setColumn(i, ((Float) row[i]).floatValue());
              break;
            case STRING:
              dataTableBuilder.setColumn(i, ((String) row[i]));
              break;
            default:
              dataTableBuilder.setColumn(i, row[i]);
              break;
          }
        } else {
          switch (dataSchema.getColumnType(i)) {
            case INT_ARRAY:
              dataTableBuilder.setColumn(i, (int[]) row[i]);
              break;
            case LONG_ARRAY:
              dataTableBuilder.setColumn(i, (long[]) row[i]);
              break;
            case DOUBLE_ARRAY:
              dataTableBuilder.setColumn(i, (double[]) row[i]);
              break;
            case FLOAT_ARRAY:
              dataTableBuilder.setColumn(i, (float[]) row[i]);
              break;
            case STRING_ARRAY:
              dataTableBuilder.setColumn(i, (String[]) row[i]);
              break;
            default:
              dataTableBuilder.setColumn(i, row[i]);
              break;
          }
        }
      }
      dataTableBuilder.finishRow();
    }
    dataTableBuilder.seal();
    return dataTableBuilder.build();
  }

  public static String getRowStringFromSerializable(Serializable[] row, DataSchema dataSchema) {
    String rowString = "";
    if (dataSchema.getColumnType(0).isSingleValue()) {
      if (dataSchema.getColumnType(0) == DataType.STRING) {
        rowString += (String) row[0];
      } else {
        rowString += row[0];
      }
    } else {
      rowString += "[ ";
      if (dataSchema.getColumnType(0) == DataType.STRING) {
        String[] values = (String[]) row[0];
        for (int i = 0; i < values.length; ++i) {
          rowString += values[i];
        }
      } else {
        Serializable[] values = (Serializable[]) row[0];
        for (int i = 0; i < values.length; ++i) {
          rowString += values[i];
        }
      }
      rowString += " ]";
    }
    for (int i = 1; i < row.length; ++i) {
      if (dataSchema.getColumnType(i).isSingleValue()) {
        if (dataSchema.getColumnType(i) == DataType.STRING) {
          rowString += " : " + (String) row[i];
        } else {
          rowString += " : " + row[i];
        }
      } else {

        rowString += " : [ ";
        switch (dataSchema.getColumnType(i)) {
          case STRING_ARRAY:
            String[] stringValues = (String[]) row[i];
            for (int j = 0; j < stringValues.length; ++j) {
              rowString += stringValues[j] + " ";
            }
            break;
          case INT_ARRAY:
            int[] intValues = (int[]) row[i];
            for (int j = 0; j < intValues.length; ++j) {
              rowString += intValues[j] + " ";
            }
            break;
          case FLOAT_ARRAY:
            float[] floatValues = (float[]) row[i];
            for (int j = 0; j < floatValues.length; ++j) {
              rowString += floatValues[j] + " ";
            }
            break;
          case LONG_ARRAY:
            long[] longValues = (long[]) row[i];
            for (int j = 0; j < longValues.length; ++j) {
              rowString += longValues[j] + " ";
            }
            break;
          case DOUBLE_ARRAY:
            double[] doubleValues = (double[]) row[i];
            for (int j = 0; j < doubleValues.length; ++j) {
              rowString += doubleValues[j] + " ";
            }
            break;

          default:
            break;
        }
        rowString += "]";
      }
    }
    return rowString;
  }
}
