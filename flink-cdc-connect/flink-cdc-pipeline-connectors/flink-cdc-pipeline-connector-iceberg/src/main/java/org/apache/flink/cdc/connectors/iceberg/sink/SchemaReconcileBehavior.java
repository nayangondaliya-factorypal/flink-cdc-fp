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

/**
 * Policy that determines how {@link IcebergMetadataApplier} reconciles an existing Iceberg table
 * schema with the schema carried by an incoming {@code CreateTableEvent}.
 *
 * <p>Independently of this policy, the Iceberg writer always projects incoming CDC records onto
 * the persisted Iceberg column layout so that a mismatch cannot cause positional index-out-of-bounds
 * failures while writing.
 */
public enum SchemaReconcileBehavior {

    /**
     * Do not touch the existing table at all on {@code CreateTableEvent}. The user is expected to
     * manage the Iceberg schema out-of-band. The writer will still project incoming records onto
     * the persisted layout and emit NULL for any columns that only exist on the Iceberg side.
     */
    OFF,

    /**
     * Extend the Iceberg schema to cover the incoming CDC schema without removing anything:
     *
     * <ul>
     *   <li>Columns present in the CDC schema but missing from Iceberg are appended.
     *   <li>Columns whose primitive types differ are updated via Iceberg type promotion (best
     *       effort).
     *   <li>Columns present in Iceberg but absent from the CDC schema are preserved; the writer
     *       emits NULL for those columns.
     *   <li>Column order is never changed.
     * </ul>
     */
    ADDITIVE,

    /**
     * Force the existing Iceberg schema to match the incoming CDC schema exactly: add missing
     * columns, drop extra columns, alter primitive type mismatches, and reorder columns to match
     * the CDC layout. This may remove columns a user deliberately kept around.
     */
    STRICT;

    /** Parse a user-provided string, falling back to {@link #ADDITIVE} on null/unknown values. */
    public static SchemaReconcileBehavior from(String raw) {
        if (raw == null) {
            return ADDITIVE;
        }
        switch (raw.trim().toLowerCase()) {
            case "off":
                return OFF;
            case "additive":
                return ADDITIVE;
            case "strict":
                return STRICT;
            default:
                throw new IllegalArgumentException(
                        "Unknown value for sink.schema.reconcile-on-create.behavior: '"
                                + raw
                                + "'. Expected one of: off, additive, strict.");
        }
    }
}
