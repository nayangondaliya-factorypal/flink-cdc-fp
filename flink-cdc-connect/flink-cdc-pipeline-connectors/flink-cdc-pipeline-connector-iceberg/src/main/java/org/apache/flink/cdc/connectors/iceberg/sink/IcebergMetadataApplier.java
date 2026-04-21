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

package org.apache.flink.cdc.connectors.iceberg.sink;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.visitor.SchemaChangeEventVisitor;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.utils.DataTypeUtils;
import org.apache.flink.cdc.connectors.iceberg.sink.utils.HadoopConfUtils;
import org.apache.flink.cdc.connectors.iceberg.sink.utils.IcebergTypeUtils;

import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.cdc.common.utils.Preconditions.checkNotNull;

/** A {@link MetadataApplier} for Apache Iceberg. */
public class IcebergMetadataApplier implements MetadataApplier {
    private static final Pattern PARTITION_YEAR_PATTERN = Pattern.compile("^year\\((.*)\\)$");

    private static final Pattern PARTITION_MONTH_PATTERN = Pattern.compile("^month\\((.*)\\)$");

    private static final Pattern PARTITION_DAY_PATTERN = Pattern.compile("^day\\((.*)\\)$");

    private static final Pattern PARTITION_HOUR_PATTERN = Pattern.compile("^hour\\((.*)\\)$");

    private static final Pattern PARTITION_BUCKET_PATTERN =
            Pattern.compile("^bucket\\[(\\d+)]\\((.*)\\)$");

    private static final Pattern PARTITION_TRUNCATE_PATTERN =
            Pattern.compile("^truncate\\[(\\d+)]\\((.*)\\)$");

    private static final Logger LOG = LoggerFactory.getLogger(IcebergMetadataApplier.class);

    private transient Catalog catalog;

    private final Map<String, String> catalogOptions;

    // currently, we set table options for all tables using the same options.
    private final Map<String, String> tableOptions;

    private final Map<TableId, List<String>> partitionMaps;

    private final Map<String, String> hadoopConfOptions;

    private Set<SchemaChangeEventType> enabledSchemaEvolutionTypes;

    private final SchemaReconcileBehavior reconcileBehavior;

    public IcebergMetadataApplier(Map<String, String> catalogOptions) {
        this(catalogOptions, new HashMap<>(), new HashMap<>(), null);
    }

    public IcebergMetadataApplier(
            Map<String, String> catalogOptions,
            Map<String, String> tableOptions,
            Map<TableId, List<String>> partitionMaps) {
        this(catalogOptions, tableOptions, partitionMaps, null);
    }

    public IcebergMetadataApplier(
            Map<String, String> catalogOptions,
            Map<String, String> tableOptions,
            Map<TableId, List<String>> partitionMaps,
            Map<String, String> hadoopConfOptions) {
        this(
                catalogOptions,
                tableOptions,
                partitionMaps,
                hadoopConfOptions,
                SchemaReconcileBehavior.ADDITIVE);
    }

    public IcebergMetadataApplier(
            Map<String, String> catalogOptions,
            Map<String, String> tableOptions,
            Map<TableId, List<String>> partitionMaps,
            Map<String, String> hadoopConfOptions,
            SchemaReconcileBehavior reconcileBehavior) {
        this.catalogOptions = catalogOptions;
        this.tableOptions = tableOptions;
        this.partitionMaps = partitionMaps;
        this.hadoopConfOptions = hadoopConfOptions;
        this.enabledSchemaEvolutionTypes = getSupportedSchemaEvolutionTypes();
        this.reconcileBehavior =
                reconcileBehavior == null ? SchemaReconcileBehavior.ADDITIVE : reconcileBehavior;
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent)
            throws SchemaEvolveException {
        if (catalog == null) {
            Configuration configuration = HadoopConfUtils.createConfiguration(hadoopConfOptions);
            catalog =
                    CatalogUtil.buildIcebergCatalog(
                            this.getClass().getSimpleName(), catalogOptions, configuration);
        }
        SchemaChangeEventVisitor.visit(
                schemaChangeEvent,
                addColumnEvent -> {
                    applyAddColumn(addColumnEvent);
                    return null;
                },
                alterColumnTypeEvent -> {
                    applyAlterColumnType(alterColumnTypeEvent);
                    return null;
                },
                createTableEvent -> {
                    applyCreateTable(createTableEvent);
                    return null;
                },
                dropColumnEvent -> {
                    applyDropColumn(dropColumnEvent);
                    return null;
                },
                dropTableEvent -> {
                    throw new UnsupportedSchemaChangeEventException(dropTableEvent);
                },
                renameColumnEvent -> {
                    applyRenameColumn(renameColumnEvent);
                    return null;
                },
                truncateTableEvent -> {
                    throw new UnsupportedSchemaChangeEventException(truncateTableEvent);
                },
                alterTableEvent -> {
                    throw new UnsupportedSchemaChangeEventException(alterTableEvent);
                });
    }

    private void applyCreateTable(CreateTableEvent event) {
        try {
            long startTimestamp = System.currentTimeMillis();
            TableIdentifier tableIdentifier = TableIdentifier.parse(event.tableId().identifier());
            // Step 0: Create namespace if not exists.
            if (catalog instanceof SupportsNamespaces) {
                SupportsNamespaces namespaceCatalog = (SupportsNamespaces) catalog;
                Namespace namespace = Namespace.of(tableIdentifier.namespace().levels());
                if (!namespaceCatalog.namespaceExists(namespace)) {
                    namespaceCatalog.createNamespace(namespace);
                }
            }

            // Step1: Build Schema.
            org.apache.flink.cdc.common.schema.Schema cdcSchema = event.getSchema();
            List<Types.NestedField> columns = new ArrayList<>();
            Set<Integer> identifierFieldIds = new HashSet<>();
            for (int index = 0; index < event.getSchema().getColumnCount(); index++) {
                columns.add(
                        IcebergTypeUtils.convertCdcColumnToIcebergField(
                                index, (PhysicalColumn) cdcSchema.getColumns().get(index)));
                if (cdcSchema.primaryKeys().contains(cdcSchema.getColumns().get(index).getName())) {
                    identifierFieldIds.add(index);
                }
            }

            // Step2: Build partition spec.
            Schema icebergSchema = new Schema(columns, identifierFieldIds);
            List<String> partitionColumns = cdcSchema.partitionKeys();
            if (partitionMaps.containsKey(event.tableId())) {
                partitionColumns = partitionMaps.get(event.tableId());
            }
            PartitionSpec partitionSpec = generatePartitionSpec(icebergSchema, partitionColumns);
            if (!catalog.tableExists(tableIdentifier)) {
                Table table =
                        catalog.createTable(
                                tableIdentifier, icebergSchema, partitionSpec, tableOptions);

                applyDefaultValues(table, cdcSchema);

                LOG.info(
                        "Spend {} ms to create iceberg table {}",
                        System.currentTimeMillis() - startTimestamp,
                        tableIdentifier);
            } else {
                // Table already exists in the catalog. Its persisted schema may diverge from
                // the incoming pipeline schema (e.g. the table was created by a previous run
                // with different projection/aliases, the user deliberately kept extra
                // columns, or upstream DDL occurred while this job was not running).
                //
                // How we react depends on reconcileBehavior:
                //  - OFF:      leave the Iceberg schema untouched. The writer will still
                //              project incoming records onto the persisted layout so a
                //              mismatch does not blow up positional RowData access.
                //  - ADDITIVE: add missing columns and alter primitive types if allowed;
                //              never drop or reorder. Extra Iceberg columns are preserved
                //              and the writer pads them with NULL.
                //  - STRICT:   force the Iceberg schema to match CDC exactly (add/drop/
                //              alter/reorder). May remove columns kept on purpose.
                Table table = catalog.loadTable(tableIdentifier);
                if (reconcileBehavior == SchemaReconcileBehavior.OFF) {
                    LOG.info(
                            "Iceberg table {} already exists and "
                                    + "sink.schema.reconcile-on-create.behavior=off; leaving schema untouched.",
                            tableIdentifier);
                } else {
                    reconcileExistingTable(table, cdcSchema, event.tableId());
                    applyDefaultValues(table, cdcSchema);
                }

                LOG.info(
                        "Spend {} ms to reconcile existing iceberg table {} (behavior={})",
                        System.currentTimeMillis() - startTimestamp,
                        tableIdentifier,
                        reconcileBehavior);
            }
        } catch (Exception e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }

    /**
     * Aligns the persisted Iceberg table schema with the incoming CDC schema. Behavior depends on
     * {@link #reconcileBehavior}:
     *
     * <ul>
     *   <li>{@link SchemaReconcileBehavior#ADDITIVE} (default) - add missing CDC columns and alter
     *       primitive-type mismatches (both gated on {@link #enabledSchemaEvolutionTypes}). Never
     *       drop Iceberg-only columns, never reorder; the sink writer pads Iceberg-only columns
     *       with NULL instead. Safe for tables that intentionally keep extra columns.
     *   <li>{@link SchemaReconcileBehavior#STRICT} - in addition to the above, also drop columns
     *       absent from the CDC schema and reorder columns to match the CDC order. This can delete
     *       deliberately-retained columns.
     * </ul>
     *
     * <p>{@link SchemaReconcileBehavior#OFF} never enters this method; callers short-circuit
     * before it is invoked.
     */
    private void reconcileExistingTable(
            Table table,
            org.apache.flink.cdc.common.schema.Schema cdcSchema,
            TableId tableId) {
        Schema currentIcebergSchema = table.schema();
        List<Column> cdcColumns = cdcSchema.getColumns();

        Set<String> cdcColumnNames = new HashSet<>();
        for (Column column : cdcColumns) {
            cdcColumnNames.add(column.getName());
        }

        Set<String> icebergColumnNames = new HashSet<>();
        for (Types.NestedField field : currentIcebergSchema.columns()) {
            icebergColumnNames.add(field.name());
        }

        UpdateSchema updateSchema = table.updateSchema();
        boolean hasChanges = false;

        boolean canAdd = enabledSchemaEvolutionTypes.contains(SchemaChangeEventType.ADD_COLUMN);
        boolean canDrop = enabledSchemaEvolutionTypes.contains(SchemaChangeEventType.DROP_COLUMN);
        boolean canAlter =
                enabledSchemaEvolutionTypes.contains(SchemaChangeEventType.ALTER_COLUMN_TYPE);
        boolean dropExtras = reconcileBehavior == SchemaReconcileBehavior.STRICT && canDrop;
        boolean reorder = reconcileBehavior == SchemaReconcileBehavior.STRICT;

        // Extra Iceberg columns: drop only in STRICT+DROP_COLUMN. Otherwise preserve and log.
        for (String icebergColumn : icebergColumnNames) {
            if (cdcColumnNames.contains(icebergColumn)) {
                continue;
            }
            if (dropExtras) {
                LOG.info(
                        "Dropping Iceberg column '{}' for table {} (strict mode) because it is not "
                                + "present in the incoming CDC schema.",
                        icebergColumn,
                        tableId);
                updateSchema.deleteColumn(icebergColumn);
                hasChanges = true;
            } else {
                LOG.info(
                        "Iceberg column '{}' for table {} is not present in the incoming CDC schema; "
                                + "preserving it (behavior={}, DROP_COLUMN enabled={}). The sink writer "
                                + "will emit NULL for this column.",
                        icebergColumn,
                        tableId,
                        reconcileBehavior,
                        canDrop);
            }
        }

        // Add missing CDC columns; alter primitive type mismatches when allowed.
        for (Column cdcColumn : cdcColumns) {
            Types.NestedField existing = currentIcebergSchema.findField(cdcColumn.getName());
            Type newIcebergType =
                    FlinkSchemaUtil.convert(
                            DataTypeUtils.toFlinkDataType(cdcColumn.getType()).getLogicalType());

            if (existing == null) {
                if (!canAdd) {
                    LOG.warn(
                            "CDC column '{}' is missing from Iceberg table {} but ADD_COLUMN is "
                                    + "disabled. The sink writer will drop this column from written rows.",
                            cdcColumn.getName(),
                            tableId);
                    continue;
                }
                LOG.info(
                        "Adding missing Iceberg column '{}' (type {}) for table {}.",
                        cdcColumn.getName(),
                        newIcebergType,
                        tableId);
                updateSchema.addColumn(
                        cdcColumn.getName(), newIcebergType, cdcColumn.getComment());
                hasChanges = true;
            } else if (!existing.type().equals(newIcebergType)) {
                if (!canAlter) {
                    LOG.warn(
                            "Iceberg column '{}' for table {} has type {} but CDC schema expects {}; "
                                    + "ALTER_COLUMN_TYPE is disabled so leaving as-is.",
                            cdcColumn.getName(),
                            tableId,
                            existing.type(),
                            newIcebergType);
                    continue;
                }
                if (!newIcebergType.isPrimitiveType()) {
                    LOG.warn(
                            "Cannot reconcile Iceberg column '{}' for table {}: Iceberg only "
                                    + "supports altering primitive column types, but incoming type is {}.",
                            cdcColumn.getName(),
                            tableId,
                            newIcebergType);
                    continue;
                }
                try {
                    updateSchema.updateColumn(
                            cdcColumn.getName(), newIcebergType.asPrimitiveType());
                    hasChanges = true;
                    LOG.info(
                            "Altering Iceberg column '{}' for table {} from {} to {}.",
                            cdcColumn.getName(),
                            tableId,
                            existing.type(),
                            newIcebergType);
                } catch (IllegalArgumentException e) {
                    // Iceberg disallows certain promotions (e.g. int->string). Skip and warn.
                    LOG.warn(
                            "Iceberg rejected altering column '{}' for table {} from {} to {}: {}",
                            cdcColumn.getName(),
                            tableId,
                            existing.type(),
                            newIcebergType,
                            e.getMessage());
                }
            }
        }

        // Reorder to the CDC layout only in STRICT. In ADDITIVE, the writer's projection layer
        // handles positional mismatches without touching Iceberg metadata.
        if (reorder) {
            for (int i = 0; i < cdcColumns.size(); i++) {
                String columnName = cdcColumns.get(i).getName();
                if (!cdcColumnNames.contains(columnName)) {
                    continue;
                }
                if (i == 0) {
                    updateSchema.moveFirst(columnName);
                } else {
                    updateSchema.moveAfter(columnName, cdcColumns.get(i - 1).getName());
                }
            }
            hasChanges = true;
        }

        if (hasChanges) {
            updateSchema.commit();
            LOG.info(
                    "Reconciled Iceberg schema for table {} (behavior={}).",
                    tableId,
                    reconcileBehavior);
        } else {
            LOG.info(
                    "Iceberg schema for table {} already covers the incoming CDC schema; "
                            + "no reconciliation needed (behavior={}).",
                    tableId,
                    reconcileBehavior);
        }
    }

    private void applyDefaultValues(
            Table table, org.apache.flink.cdc.common.schema.Schema cdcSchema) {
        if (getFormatVersion(table) < 3) {
            return;
        }
        UpdateSchema updateSchema = null;
        for (Column column : cdcSchema.getColumns()) {
            Literal<?> defaultValue =
                    IcebergTypeUtils.parseDefaultValue(
                            column.getDefaultValueExpression(), column.getType());
            if (defaultValue != null) {
                if (updateSchema == null) {
                    updateSchema = table.updateSchema();
                }
                updateSchema.updateColumnDefault(column.getName(), defaultValue);
            }
        }
        if (updateSchema != null) {
            updateSchema.commit();
        }
    }

    private void applyAddColumn(AddColumnEvent event) {
        TableIdentifier tableIdentifier = TableIdentifier.parse(event.tableId().identifier());
        try {
            Table table = catalog.loadTable(tableIdentifier);
            applyAddColumnEventWithPosition(table, event);
        } catch (Exception e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }

    private void applyAddColumnEventWithPosition(Table table, AddColumnEvent event)
            throws SchemaEvolveException {

        try {
            UpdateSchema updateSchema = table.updateSchema();
            for (AddColumnEvent.ColumnWithPosition columnWithPosition : event.getAddedColumns()) {
                Column addColumn = columnWithPosition.getAddColumn();
                String columnName = addColumn.getName();
                String columnComment = addColumn.getComment();
                Type icebergType =
                        FlinkSchemaUtil.convert(
                                DataTypeUtils.toFlinkDataType(addColumn.getType())
                                        .getLogicalType());
                Literal<?> defaultValue =
                        IcebergTypeUtils.parseDefaultValue(
                                addColumn.getDefaultValueExpression(), addColumn.getType());
                if (defaultValue != null && getFormatVersion(table) >= 3) {
                    updateSchema.addColumn(columnName, icebergType, columnComment, defaultValue);
                    updateSchema.updateColumnDefault(columnName, defaultValue);
                } else {
                    updateSchema.addColumn(columnName, icebergType, columnComment);
                }
                switch (columnWithPosition.getPosition()) {
                    case FIRST:
                        updateSchema.moveFirst(columnName);
                        break;
                    case LAST:
                        break;
                    case BEFORE:
                        checkNotNull(
                                columnWithPosition.getExistedColumnName(),
                                "Existing column name must be provided for BEFORE position");
                        updateSchema.moveBefore(
                                columnName, columnWithPosition.getExistedColumnName());
                        break;
                    case AFTER:
                        checkNotNull(
                                columnWithPosition.getExistedColumnName(),
                                "Existing column name must be provided for AFTER position");
                        updateSchema.moveAfter(
                                columnName, columnWithPosition.getExistedColumnName());
                        break;
                    default:
                        throw new SchemaEvolveException(
                                event,
                                "Unknown column position: " + columnWithPosition.getPosition());
                }
            }
            updateSchema.commit();
        } catch (Exception e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }

    private void applyDropColumn(DropColumnEvent event) {
        try {
            UpdateSchema updateSchema =
                    catalog.loadTable(TableIdentifier.parse(event.tableId().identifier()))
                            .updateSchema();
            event.getDroppedColumnNames().forEach(updateSchema::deleteColumn);
            updateSchema.commit();
        } catch (Exception e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }

    private void applyRenameColumn(RenameColumnEvent event) {
        try {
            UpdateSchema updateSchema =
                    catalog.loadTable(TableIdentifier.parse(event.tableId().identifier()))
                            .updateSchema();
            event.getNameMapping().forEach(updateSchema::renameColumn);
            updateSchema.commit();
        } catch (Exception e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }

    private void applyAlterColumnType(AlterColumnTypeEvent event) {
        try {
            UpdateSchema updateSchema =
                    catalog.loadTable(TableIdentifier.parse(event.tableId().identifier()))
                            .updateSchema();
            event.getTypeMapping()
                    .forEach(
                            (name, newType) -> {
                                Type.PrimitiveType type =
                                        FlinkSchemaUtil.convert(
                                                        DataTypeUtils.toFlinkDataType(newType)
                                                                .getLogicalType())
                                                .asPrimitiveType();
                                updateSchema.updateColumn(name, type);
                            });
            updateSchema.commit();
        } catch (Exception e) {
            throw new SchemaEvolveException(event, e.getMessage(), e);
        }
    }

    private PartitionSpec generatePartitionSpec(Schema schema, List<String> partitionColumns) {
        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        for (String name : partitionColumns) {
            Matcher matcherYear = PARTITION_YEAR_PATTERN.matcher(name);
            if (matcherYear.matches()) {
                String matchedName = matcherYear.group(1);
                builder.year(matchedName);
                continue;
            }

            Matcher matcherMonth = PARTITION_MONTH_PATTERN.matcher(name);
            if (matcherMonth.matches()) {
                String matchedName = matcherMonth.group(1);
                builder.month(matchedName);
                continue;
            }

            Matcher matcherDay = PARTITION_DAY_PATTERN.matcher(name);
            if (matcherDay.matches()) {
                String matchedName = matcherDay.group(1);
                builder.day(matchedName);
                continue;
            }

            Matcher matcherHour = PARTITION_HOUR_PATTERN.matcher(name);
            if (matcherHour.matches()) {
                String matchedName = matcherHour.group(1);
                builder.hour(matchedName);
                continue;
            }

            Matcher matcherBucket = PARTITION_BUCKET_PATTERN.matcher(name);
            if (matcherBucket.matches()) {
                String matchedName = matcherBucket.group(2);
                int numBuckets = Integer.parseInt(matcherBucket.group(1));
                builder.bucket(matchedName, numBuckets);
                continue;
            }

            Matcher matcherTruncate = PARTITION_TRUNCATE_PATTERN.matcher(name);
            if (matcherTruncate.matches()) {
                String matchedName = matcherTruncate.group(2);
                int width = Integer.parseInt(matcherTruncate.group(1));
                builder.truncate(matchedName, width);
                continue;
            }

            builder.identity(name);
        }
        return builder.build();
    }

    @Override
    public MetadataApplier setAcceptedSchemaEvolutionTypes(
            Set<SchemaChangeEventType> schemaEvolutionTypes) {
        this.enabledSchemaEvolutionTypes = schemaEvolutionTypes;
        return this;
    }

    @Override
    public boolean acceptsSchemaEvolutionType(SchemaChangeEventType schemaChangeEventType) {
        return enabledSchemaEvolutionTypes.contains(schemaChangeEventType);
    }

    @Override
    public Set<SchemaChangeEventType> getSupportedSchemaEvolutionTypes() {
        return Sets.newHashSet(
                SchemaChangeEventType.CREATE_TABLE,
                SchemaChangeEventType.ADD_COLUMN,
                SchemaChangeEventType.DROP_COLUMN,
                SchemaChangeEventType.RENAME_COLUMN,
                SchemaChangeEventType.ALTER_COLUMN_TYPE);
    }

    private int getFormatVersion(Table table) {
        if (table instanceof HasTableOperations) {
            return ((HasTableOperations) table).operations().current().formatVersion();
        }
        return 2;
    }

    @Override
    public void close() {
        catalog = null;
    }
}
