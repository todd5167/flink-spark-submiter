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

package cn.todd.flink.executor;

import cn.todd.flink.config.StreamEnvConfigManager;
import cn.todd.flink.parser.ParamsInfo;
import cn.todd.flink.parser.SqlCommandParser;
import cn.todd.flink.parser.SqlCommandParser.SqlCommandCall;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Properties;

/**
 * @author todd5167
 */
public class SqlExecutor {
    // --------------------------------------------------------------------------------------------
    private final ParamsInfo paramsInfo;
    private TableEnvironment tableEnvironment;

    public SqlExecutor(ParamsInfo paramsInfo) {
        this.paramsInfo = paramsInfo;
    }

    public void run() throws Exception {
        tableEnvironment = getStreamTableEnv(paramsInfo.getConfProp());
        List<SqlCommandCall> calls = SqlCommandParser.parseSqlText(paramsInfo.getSqlText());
        calls.stream().forEach(this::callCommand);
    }


    public static StreamTableEnvironment getStreamTableEnv(Properties confProperties)
            throws NoSuchMethodException, IOException, IllegalAccessException, InvocationTargetException {
        // build stream exec env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamEnvConfigManager.streamExecutionEnvironmentConfig(env, confProperties);

        // use blink and stream mode
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironmentImpl.create(env, settings, new TableConfig());
        StreamEnvConfigManager.streamTableEnvironmentStateTTLConfig(tableEnv, confProperties);
        StreamEnvConfigManager.streamTableEnvironmentEarlyTriggerConfig(tableEnv, confProperties);
        return tableEnv;
    }


    // --------------------------------------------------------------------------------------------
    // TODO SUPPORT FUNCTION
    private void callCommand(SqlCommandCall cmdCall) {
        switch (cmdCall.command) {
            case SET:
                callSet(cmdCall);
                break;
            case CREATE_TABLE:
            case CREATE_VIEW:
                callCreateTableOrView(cmdCall);
                break;
            case INSERT_INTO:
                callInsertInto(cmdCall);
                break;
            default:
                throw new RuntimeException("Unsupported command: " + cmdCall.command);
        }
    }

    private void callSet(SqlCommandCall cmdCall) {
        String key = cmdCall.operands[0];
        String value = cmdCall.operands[1];
        tableEnvironment.getConfig().getConfiguration().setString(key, value);
    }

    private void callCreateTableOrView(SqlCommandCall cmdCall) {
        String ddl = cmdCall.operands[0];
        try {
            tableEnvironment.executeSql(ddl);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + ddl + "\n", e);
        }
    }

    private void callInsertInto(SqlCommandCall cmdCall) {
        String dml = cmdCall.operands[0];
        try {
            tableEnvironment.executeSql(dml);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + dml + "\n", e);
        }
    }

}
