/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.todd.flink.connectors.randomdata.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.api.functions.source.StatefulSequenceSource;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.datagen.DataGenTableSource;
import org.apache.flink.table.factories.datagen.types.RowDataGenerator;
import org.apache.flink.table.sources.StreamTableSource;

/**
 * A {@link StreamTableSource} that emits each number from a given interval exactly once, possibly
 * in parallel. See {@link StatefulSequenceSource}.
 */
@Internal
public class RandomDataGenTableSource implements ScanTableSource {

    private final DataGenerator<?>[] fieldGenerators;
    private final String tableName;
    private final TableSchema schema;
    private final long rowsPerSecond;
    private final Long numberOfRows;

    public RandomDataGenTableSource(
            DataGenerator<?>[] fieldGenerators,
            String tableName,
            TableSchema schema,
            long rowsPerSecond,
            Long numberOfRows) {
        this.fieldGenerators = fieldGenerators;
        this.tableName = tableName;
        this.schema = schema;
        this.rowsPerSecond = rowsPerSecond;
        this.numberOfRows = numberOfRows;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        boolean isBounded = numberOfRows != null;
        //  generate from source function.
        return SourceFunctionProvider.of(createSource(), isBounded);
    }

    @VisibleForTesting
    public DataGeneratorSource<RowData> createSource() {
        return new DataGeneratorSource<>(
                new RowDataGenerator(fieldGenerators, schema.getFieldNames()),
                rowsPerSecond,
                numberOfRows);
    }

    @Override
    public DynamicTableSource copy() {
        return new DataGenTableSource(
                fieldGenerators, tableName, schema, rowsPerSecond, numberOfRows);
    }

    @Override
    public String asSummaryString() {
        return "RandomDataTableSource";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }
}
