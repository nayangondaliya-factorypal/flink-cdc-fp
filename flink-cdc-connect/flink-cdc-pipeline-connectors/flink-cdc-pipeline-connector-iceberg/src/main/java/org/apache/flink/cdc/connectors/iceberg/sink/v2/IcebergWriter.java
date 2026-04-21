/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.iceberg.sink.v2;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.iceberg.sink.utils.HadoopConfUtils;
import org.apache.flink.cdc.connectors.iceberg.sink.utils.RowDataUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A {@link SinkWriter} for Apache Iceberg. */
public class IcebergWriter
        implements CommittingSinkWriter<Event, WriteResultWrapper>,
                StatefulSinkWriter<Event, IcebergWriterState> {

    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergWriter.class);

    public static final String DEFAULT_FILE_FORMAT = "parquet";

    public static final long DEFAULT_MAX_FILE_SIZE = 256 * 1024 * 1024;

    private Map<TableId, RowDataTaskWriterFactory> writerFactoryMap;

    private Map<TableId, TaskWriter<RowData>> writerMap;

    private Map<TableId, TableSchemaWrapper> schemaMap;

    /**
     * Per-table projection from the CDC RowData layout (produced by
     * {@link RowDataUtils#convertDataChangeEventToRowData}) into the persisted Iceberg table
     * layout. Populated lazily on the first DataChangeEvent for each table and refreshed whenever
     * a SchemaChangeEvent arrives. A {@code null} entry signals that the two layouts already
     * match exactly so no projection is necessary.
     */
    private Map<TableId, CdcToIcebergProjector> projectorMap;

    private final List<WriteResultWrapper> temporaryWriteResult;

    private Catalog catalog;

    private final int taskId;

    private final int attemptId;

    private final ZoneId zoneId;

    private long lastCheckpointId;

    private final String jobId;

    private final String operatorId;

    public IcebergWriter(
            Map<String, String> catalogOptions,
            int taskId,
            int attemptId,
            ZoneId zoneId,
            long lastCheckpointId,
            String jobId,
            String operatorId,
            Map<String, String> hadoopConfOptions) {
        Configuration configuration = HadoopConfUtils.createConfiguration(hadoopConfOptions);
        catalog =
                CatalogUtil.buildIcebergCatalog(
                        this.getClass().getSimpleName(), catalogOptions, configuration);
        writerFactoryMap = new HashMap<>();
        writerMap = new HashMap<>();
        schemaMap = new HashMap<>();
        projectorMap = new HashMap<>();
        temporaryWriteResult = new ArrayList<>();
        this.taskId = taskId;
        this.attemptId = attemptId;
        this.zoneId = zoneId;
        this.lastCheckpointId = lastCheckpointId;
        this.jobId = jobId;
        this.operatorId = operatorId;
        LOGGER.info(
                "IcebergWriter created, taskId: {}, attemptId: {}, lastCheckpointId: {}, jobId: {}, operatorId: {}",
                taskId,
                attemptId,
                lastCheckpointId,
                jobId,
                operatorId);
    }

    @Override
    public List<IcebergWriterState> snapshotState(long checkpointId) {
        return Collections.singletonList(new IcebergWriterState(jobId, operatorId));
    }

    @Override
    public Collection<WriteResultWrapper> prepareCommit() throws IOException {
        List<WriteResultWrapper> list = new ArrayList<>();
        list.addAll(temporaryWriteResult);
        list.addAll(getWriteResult());
        temporaryWriteResult.clear();
        lastCheckpointId++;
        return list;
    }

    private RowDataTaskWriterFactory getRowDataTaskWriterFactory(TableId tableId) {
        Table table = catalog.loadTable(TableIdentifier.parse(tableId.identifier()));
        RowType rowType = FlinkSchemaUtil.convert(table.schema());
        // Whenever a writer factory is (re)built we also refresh the CDC -> Iceberg projector
        // so it reflects the Iceberg schema this factory will write against. Without this,
        // a reconcile that changed Iceberg's column layout after a previous DataChangeEvent
        // would leave the projector stale and the writer would either pad the wrong positions
        // or throw ArrayIndexOutOfBoundsException on the first subsequent record.
        TableSchemaWrapper tableSchemaWrapper = schemaMap.get(tableId);
        if (tableSchemaWrapper != null) {
            projectorMap.put(
                    tableId,
                    CdcToIcebergProjector.build(tableSchemaWrapper.getSchema(), table.schema()));
        }
        RowDataTaskWriterFactory rowDataTaskWriterFactory =
                new RowDataTaskWriterFactory(
                        table,
                        rowType,
                        DEFAULT_MAX_FILE_SIZE,
                        FileFormat.fromString(DEFAULT_FILE_FORMAT),
                        new HashMap<>(),
                        new ArrayList<>(table.schema().identifierFieldIds()),
                        true);
        rowDataTaskWriterFactory.initialize(taskId, attemptId);
        return rowDataTaskWriterFactory;
    }

    @Override
    public void write(Event event, Context context) throws IOException {
        if (event instanceof DataChangeEvent) {
            DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
            TableId tableId = dataChangeEvent.tableId();
            writerFactoryMap.computeIfAbsent(tableId, this::getRowDataTaskWriterFactory);
            TaskWriter<RowData> writer =
                    writerMap.computeIfAbsent(
                            tableId, tableId1 -> writerFactoryMap.get(tableId1).create());
            TableSchemaWrapper tableSchemaWrapper = schemaMap.get(tableId);
            RowData cdcRowData =
                    RowDataUtils.convertDataChangeEventToRowData(
                            dataChangeEvent, tableSchemaWrapper.getFieldGetters());
            // Project onto the Iceberg layout so positional access (which
            // FlinkParquetWriters performs through Iceberg's table schema)
            // cannot go out of bounds when the two layouts diverge.
            CdcToIcebergProjector projector = projectorMap.get(tableId);
            RowData rowData = projector == null ? cdcRowData : projector.project(cdcRowData);
            writer.write(rowData);
        } else {
            SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
            TableId tableId = schemaChangeEvent.tableId();
            TableSchemaWrapper tableSchemaWrapper = schemaMap.get(tableId);

            Schema newSchema =
                    tableSchemaWrapper != null
                            ? SchemaUtils.applySchemaChangeEvent(
                                    tableSchemaWrapper.getSchema(), schemaChangeEvent)
                            : SchemaUtils.applySchemaChangeEvent(null, schemaChangeEvent);
            schemaMap.put(tableId, new TableSchemaWrapper(newSchema, zoneId));
            // The CDC schema just changed; invalidate the cached projector so it is rebuilt
            // against the refreshed CDC layout on the next data event.
            projectorMap.remove(tableId);
        }
    }

    /**
     * Positional projector from the CDC RowData layout to the Iceberg table layout.
     *
     * <p>For every Iceberg column (in Iceberg's declared order), this holds the index of the
     * matching CDC column, or {@code -1} when Iceberg has a column that the CDC schema does not
     * cover. A projected row always has exactly {@link #icebergArity} fields, with NULL at any
     * position whose source CDC index is {@code -1}.
     *
     * <p>When the CDC layout already matches the Iceberg layout position-for-position,
     * {@link #build} returns {@code null} so the writer can skip the copy entirely.
     */
    private static final class CdcToIcebergProjector {

        private final int icebergArity;
        private final int[] cdcIndexPerIcebergField;

        private CdcToIcebergProjector(int icebergArity, int[] cdcIndexPerIcebergField) {
            this.icebergArity = icebergArity;
            this.cdcIndexPerIcebergField = cdcIndexPerIcebergField;
        }

        static CdcToIcebergProjector build(
                Schema cdcSchema, org.apache.iceberg.Schema icebergSchema) {
            List<Types.NestedField> icebergColumns = icebergSchema.columns();
            List<Column> cdcColumns = cdcSchema.getColumns();
            int icebergArity = icebergColumns.size();
            int[] cdcIndexPerIcebergField = new int[icebergArity];
            boolean identity = icebergArity == cdcColumns.size();
            Map<String, Integer> cdcNameToIndex = new HashMap<>();
            for (int i = 0; i < cdcColumns.size(); i++) {
                cdcNameToIndex.put(cdcColumns.get(i).getName(), i);
            }
            StringBuilder droppedFromCdc = null;
            for (int i = 0; i < icebergArity; i++) {
                String icebergName = icebergColumns.get(i).name();
                Integer cdcIdx = cdcNameToIndex.remove(icebergName);
                if (cdcIdx == null) {
                    cdcIndexPerIcebergField[i] = -1;
                    identity = false;
                } else {
                    cdcIndexPerIcebergField[i] = cdcIdx;
                    if (cdcIdx != i) {
                        identity = false;
                    }
                }
            }
            if (!cdcNameToIndex.isEmpty()) {
                droppedFromCdc = new StringBuilder();
                for (String unmapped : cdcNameToIndex.keySet()) {
                    if (droppedFromCdc.length() > 0) {
                        droppedFromCdc.append(", ");
                    }
                    droppedFromCdc.append(unmapped);
                }
                LOGGER.warn(
                        "CDC schema contains columns [{}] that are not present in the Iceberg "
                                + "table schema; those values will be dropped while writing. "
                                + "Enable ADD_COLUMN in include.schema.changes or switch to "
                                + "sink.schema.reconcile-on-create.behavior=strict to keep them.",
                        droppedFromCdc);
            }
            if (identity && droppedFromCdc == null) {
                return null;
            }
            return new CdcToIcebergProjector(icebergArity, cdcIndexPerIcebergField);
        }

        RowData project(RowData cdcRow) {
            GenericRowData out = new GenericRowData(icebergArity);
            out.setRowKind(cdcRow.getRowKind());
            // RowDataUtils.convertDataChangeEventToRowData always produces a GenericRowData, so
            // we can read its fields generically without having to know each column type. If a
            // custom producer ever substitutes a different RowData implementation, the fallback
            // in the else branch still copies NULL/null-coalesced values correctly.
            if (cdcRow instanceof GenericRowData) {
                GenericRowData cdcGeneric = (GenericRowData) cdcRow;
                for (int i = 0; i < icebergArity; i++) {
                    int cdcIdx = cdcIndexPerIcebergField[i];
                    if (cdcIdx < 0 || cdcIdx >= cdcGeneric.getArity()) {
                        out.setField(i, null);
                    } else {
                        out.setField(i, cdcGeneric.getField(cdcIdx));
                    }
                }
            } else {
                for (int i = 0; i < icebergArity; i++) {
                    int cdcIdx = cdcIndexPerIcebergField[i];
                    if (cdcIdx < 0
                            || cdcIdx >= cdcRow.getArity()
                            || cdcRow.isNullAt(cdcIdx)) {
                        out.setField(i, null);
                    } else {
                        // Best effort: materialize the field as a byte[] through RowData's raw
                        // accessor. This path is only taken if an unexpected RowData type slips
                        // through; in practice it is never hit.
                        out.setField(i, cdcRow.getRawValue(cdcIdx));
                    }
                }
            }
            return out;
        }
    }

    @Override
    public void flush(boolean flush) throws IOException {
        // Notice: flush method may be called many times during one checkpoint.
        temporaryWriteResult.addAll(getWriteResult());
    }

    private List<WriteResultWrapper> getWriteResult() throws IOException {
        long currentCheckpointId = lastCheckpointId + 1;
        List<WriteResultWrapper> writeResults = new ArrayList<>();
        for (Map.Entry<TableId, TaskWriter<RowData>> entry : writerMap.entrySet()) {
            WriteResultWrapper writeResultWrapper =
                    new WriteResultWrapper(
                            entry.getValue().complete(),
                            entry.getKey(),
                            currentCheckpointId,
                            jobId,
                            operatorId);
            writeResults.add(writeResultWrapper);
            LOGGER.info(writeResultWrapper.buildDescription());
        }
        writerMap.clear();
        writerFactoryMap.clear();
        return writeResults;
    }

    @Override
    public void writeWatermark(Watermark watermark) {}

    @Override
    public void close() throws Exception {
        if (schemaMap != null) {
            schemaMap.clear();
            schemaMap = null;
        }

        if (projectorMap != null) {
            projectorMap.clear();
            projectorMap = null;
        }

        if (writerMap != null) {
            for (TaskWriter<RowData> writer : writerMap.values()) {
                writer.close();
            }
            writerMap.clear();
            writerMap = null;
        }

        if (writerFactoryMap != null) {
            writerFactoryMap.clear();
            writerFactoryMap = null;
        }

        catalog = null;
    }
}
