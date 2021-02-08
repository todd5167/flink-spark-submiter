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

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnConfigOptionsInternal;

import cn.todd.flink.entity.JobParamsInfo;
import cn.todd.flink.enums.ETaskStatus;
import cn.todd.flink.factory.YarnClusterClientFactory;
import cn.todd.flink.utils.JobGraphBuildUtil;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Optional;

/**
 * Date: 2020/6/14
 *
 * @author todd5167
 */
public class YarnJobClusterExecutor extends AbstractClusterExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(YarnJobClusterExecutor.class);

    private static final String CONFIG_FILE_LOGBACK_NAME = "logback.xml";
    private static final String CONFIG_FILE_LOG4J_NAME = "log4j.properties";
    private static final String DEFAULT_TOTAL_PROCESS_MEMORY = "1024m";

    public YarnJobClusterExecutor(JobParamsInfo jobParamsInfo) {
        super(jobParamsInfo);
    }

    @Override
    public Optional<Pair<String, String>> submit() throws Exception {
        JobGraph jobGraph = JobGraphBuildUtil.buildJobGraph(jobParamsInfo);
        Optional.ofNullable(jobParamsInfo.getDependFile())
                .ifPresent(files -> JobGraphBuildUtil.fillDependFilesJobGraph(jobGraph, files));

        Configuration flinkConfiguration =
                JobGraphBuildUtil.getFlinkConfiguration(jobParamsInfo.getFlinkConfDir());
        appendApplicationConfig(flinkConfiguration, jobParamsInfo);

        YarnClusterDescriptor clusterDescriptor =
                (YarnClusterDescriptor)
                        YarnClusterClientFactory.INSTANCE.createClusterDescriptor(
                                jobParamsInfo.getYarnConfDir(), flinkConfiguration);

        List<File> shipFiles = findFlinkDistJar(jobParamsInfo.getFlinkJarPath(), clusterDescriptor);
        clusterDescriptor.addShipFiles(shipFiles);

        ClusterSpecification clusterSpecification =
                YarnClusterClientFactory.INSTANCE.getClusterSpecification(flinkConfiguration);
        ClusterClientProvider<ApplicationId> applicationIdClusterClientProvider =
                clusterDescriptor.deployJobCluster(clusterSpecification, jobGraph, true);

        String applicationId =
                applicationIdClusterClientProvider.getClusterClient().getClusterId().toString();
        String jobId = jobGraph.getJobID().toString();

        LOG.info("deploy per_job with appId: {}, jobId: {}", applicationId, jobId);

        return Optional.of(new Pair<>(applicationId, jobId));
    }

    @Override
    public ETaskStatus getJobStatus(String applicationId, String jobId) {
        String yarnConfDir = jobParamsInfo.getYarnConfDir();
        Preconditions.checkNotNull(applicationId, "yarn applicaitonId is not null!");
        Preconditions.checkNotNull(yarnConfDir, "yarn conf dir is not null!");

        ApplicationId appId = ConverterUtils.toApplicationId(applicationId);
        try {
            YarnClient yarnClient = YarnClusterClientFactory.INSTANCE.createYarnClient(yarnConfDir);
            ApplicationReport report = yarnClient.getApplicationReport(appId);

            YarnApplicationState applicationState = report.getYarnApplicationState();
            switch (applicationState) {
                case KILLED:
                    return ETaskStatus.KILLED;
                case NEW:
                case NEW_SAVING:
                    return ETaskStatus.CREATED;
                case SUBMITTED:
                    return ETaskStatus.SUBMITTED;
                case ACCEPTED:
                    return ETaskStatus.ACCEPTED;
                case RUNNING:
                    return ETaskStatus.RUNNING;
                case FINISHED:
                    FinalApplicationStatus finalApplicationStatus =
                            report.getFinalApplicationStatus();
                    if (finalApplicationStatus == FinalApplicationStatus.FAILED) {
                        return ETaskStatus.FAILED;
                    } else if (finalApplicationStatus == FinalApplicationStatus.SUCCEEDED) {
                        return ETaskStatus.FINISHED;
                    } else if (finalApplicationStatus == FinalApplicationStatus.KILLED) {
                        return ETaskStatus.KILLED;
                    } else if (finalApplicationStatus == FinalApplicationStatus.UNDEFINED) {
                        return ETaskStatus.FAILED;
                    } else {
                        return ETaskStatus.RUNNING;
                    }

                case FAILED:
                    return ETaskStatus.FAILED;
                default:
                    throw new RuntimeException("Unsupported application state");
            }
        } catch (YarnException | IOException e) {
            LOG.error("", e);
            return ETaskStatus.NOTFOUND;
        }
    }

    private void appendApplicationConfig(Configuration flinkConfig, JobParamsInfo jobParamsInfo) {
        if (!StringUtils.isEmpty(jobParamsInfo.getName())) {
            flinkConfig.setString(YarnConfigOptions.APPLICATION_NAME, jobParamsInfo.getName());
        }

        if (!StringUtils.isEmpty(jobParamsInfo.getQueue())) {
            flinkConfig.setString(YarnConfigOptions.APPLICATION_QUEUE, jobParamsInfo.getQueue());
        }

        if (!StringUtils.isEmpty(jobParamsInfo.getFlinkConfDir())) {
            discoverLogConfigFile(jobParamsInfo.getFlinkConfDir())
                    .ifPresent(
                            file ->
                                    flinkConfig.setString(
                                            YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE,
                                            file.getPath()));
        }

        if (!flinkConfig.contains(TaskManagerOptions.TOTAL_PROCESS_MEMORY)) {
            flinkConfig.setString(
                    TaskManagerOptions.TOTAL_PROCESS_MEMORY.key(), DEFAULT_TOTAL_PROCESS_MEMORY);
        }
    }

    private List<File> findFlinkDistJar(
            String flinkJarPath, YarnClusterDescriptor clusterDescriptor)
            throws MalformedURLException {
        if (StringUtils.isEmpty(flinkJarPath) || !new File(flinkJarPath).exists()) {
            throw new RuntimeException("The param '-flinkJarPath' ref dir is not exist");
        }
        File[] jars = new File(flinkJarPath).listFiles();
        List<File> shipFiles = Lists.newArrayList();
        for (File file : jars) {
            if (file.toURI().toURL().toString().contains("flink-dist")) {
                clusterDescriptor.setLocalJarPath(new Path(file.toURI().toURL().toString()));
            } else {
                shipFiles.add(file);
            }
        }
        return shipFiles;
    }

    private Optional<File> discoverLogConfigFile(final String configurationDirectory) {
        Optional<File> logConfigFile = Optional.empty();

        final File log4jFile =
                new File(configurationDirectory + File.separator + CONFIG_FILE_LOG4J_NAME);
        if (log4jFile.exists()) {
            logConfigFile = Optional.of(log4jFile);
        }

        final File logbackFile =
                new File(configurationDirectory + File.separator + CONFIG_FILE_LOGBACK_NAME);
        if (logbackFile.exists()) {
            if (logConfigFile.isPresent()) {
                LOG.warn(
                        "The configuration directory ('"
                                + configurationDirectory
                                + "') already contains a LOG4J config file."
                                + "If you want to use logback, then please delete or rename the log configuration file.");
            } else {
                logConfigFile = Optional.of(logbackFile);
            }
        }
        return logConfigFile;
    }
}
