/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.todd.flink.connectors.console.sink;

import cn.todd.flink.Main;
import cn.todd.flink.connectors.console.utils.TablePrintUtil;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class ConsoleOutputFormat implements OutputFormat<RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private final String[] fieldNames;
    private final DataType[] fieldDataType;


    public ConsoleOutputFormat(String[] fieldNames, DataType[] fieldDataType) {
        this.fieldNames = fieldNames;
        this.fieldDataType = fieldDataType;
    }

    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        LOG.info("taskNumber:{}", taskNumber);
        LOG.info("numTasks:{}", numTasks);
    }

    @Override
    public void writeRecord(RowData rowData) throws IOException {
        String[] recordValue = IntStream.range(0, rowData.getArity())
                .mapToObj(i -> String.valueOf(((GenericRowData) rowData).getField(i)))
                .toArray(String[]::new);

        List<String[]> data = new ArrayList<>();
        data.add(fieldNames);
        data.add(recordValue);

        TablePrintUtil.build(data).print();
    }

    @Override
    public void close() throws IOException {

    }
}
