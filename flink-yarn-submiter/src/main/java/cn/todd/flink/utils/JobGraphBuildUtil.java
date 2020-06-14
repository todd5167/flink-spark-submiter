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

package cn.todd.flink.utils;


import cn.todd.flink.entity.JobParamsInfo;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.util.Preconditions;

import java.io.File;
import java.util.Arrays;
import java.util.Properties;

/**
 *
 * 构建JobGraph 的工具类
 * Date: 2020/6/14
 * @author todd5167
 */

public class JobGraphBuildUtil {

    public static final String SAVE_POINT_PATH_KEY = "savePointPath";
    public static final String ALLOW_NON_RESTORED_STATE_KEY = "allowNonRestoredState";
    public static final String PARALLELISM = "parallelism";


    public static JobGraph buildJobGraph(JobParamsInfo jobParamsInfo) throws Exception {
        Properties confProperties = jobParamsInfo.getConfProperties();

        int parallelism = MathUtil.getIntegerVal(confProperties.getProperty(PARALLELISM, "1"));
        String flinkConfDir = jobParamsInfo.getFlinkConfDir();
        String[] execArgs = jobParamsInfo.getExecArgs();
        String runJarPath = jobParamsInfo.getRunJarPath();

        Preconditions.checkArgument(FileUtils.getFile(runJarPath).exists(), "runJarPath not exist!");

        File runJarFile = new File(runJarPath);
        SavepointRestoreSettings savepointRestoreSettings = dealSavepointRestoreSettings(jobParamsInfo.getConfProperties());

        PackagedProgram program = PackagedProgram.newBuilder()
                .setJarFile(runJarFile)
                .setArguments(execArgs)
                .setSavepointRestoreSettings(savepointRestoreSettings)
                .build();

        Configuration flinkConfig = getFlinkConfiguration(flinkConfDir);
        JobGraph jobGraph = PackagedProgramUtils.createJobGraph(program, flinkConfig, parallelism, false);

        return jobGraph;
    }

    public static Configuration getFlinkConfiguration(String flinkConfDir) {
        return StringUtils.isEmpty(flinkConfDir) ? new Configuration() : GlobalConfiguration.loadConfiguration(flinkConfDir);
    }

    private static SavepointRestoreSettings dealSavepointRestoreSettings(Properties confProperties) {
        SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.none();
        String savePointPath = confProperties.getProperty(SAVE_POINT_PATH_KEY);
        if (StringUtils.isNotBlank(savePointPath)) {
            String allowNonRestoredState = confProperties.getOrDefault(ALLOW_NON_RESTORED_STATE_KEY, "false").toString();
            savepointRestoreSettings = SavepointRestoreSettings.forPath(savePointPath, BooleanUtils.toBoolean(allowNonRestoredState));
        }
        return savepointRestoreSettings;
    }

    public static void fillDependFilesJobGraph(JobGraph jobGraph, String[] dependFiles) {
        Arrays.stream(dependFiles).forEach(path -> jobGraph.addJar(new Path("file://" + path)));
    }

}
