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

package cn.todd.flink.log;

import org.apache.flink.util.Preconditions;

import cn.todd.flink.entity.JobParamsInfo;
import cn.todd.flink.factory.YarnClusterClientFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.logaggregation.LogCLIHelpers;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;

/**
 * print all contains logs info
 *
 * @see LogCLIHelpers
 * @author todd5167
 */
public class FinishedLog {
    private static final Logger LOG = LoggerFactory.getLogger(FinishedLog.class);

    public void printAllContainersLogs(JobParamsInfo jobParamsInfo, String applicationId)
            throws IOException {
        YarnConfiguration configuration =
                YarnClusterClientFactory.INSTANCE.getYarnConf(jobParamsInfo.getYarnConfDir());

        Path remoteRootLogDir =
                new Path(
                        configuration.get(
                                YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
                                YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));
        ApplicationId appId = ConverterUtils.toApplicationId(applicationId);
        // HADOOP_USER_NAME
        String user = UserGroupInformation.getCurrentUser().getShortUserName();
        String logDirSuffix = LogAggregationUtils.getRemoteNodeLogDirSuffix(configuration);

        Path remoteAppLogDir =
                LogAggregationUtils.getRemoteAppLogDir(remoteRootLogDir, appId, user, logDirSuffix);

        long logFileSize = getLogFileSize(configuration, remoteAppLogDir.toString());
        Preconditions.checkArgument(logFileSize > 0, "log file size =0");

        RemoteIterator<FileStatus> nodeFiles = null;
        try {
            Path qualifiedLogDir =
                    FileContext.getFileContext(configuration).makeQualified(remoteAppLogDir);
            nodeFiles =
                    FileContext.getFileContext(qualifiedLogDir.toUri(), configuration)
                            .listStatus(remoteAppLogDir);
        } catch (FileNotFoundException fnf) {
            logDirNotExist(remoteAppLogDir.toString());
        }

        boolean foundAnyLogs = false;
        while (nodeFiles.hasNext()) {
            FileStatus thisNodeFile = nodeFiles.next();
            if (!thisNodeFile.getPath().getName().endsWith(LogAggregationUtils.TMP_FILE_SUFFIX)) {
                AggregatedLogFormat.LogReader reader =
                        new AggregatedLogFormat.LogReader(configuration, thisNodeFile.getPath());
                try {
                    DataInputStream valueStream;
                    AggregatedLogFormat.LogKey key = new AggregatedLogFormat.LogKey();
                    valueStream = reader.next(key);

                    while (valueStream != null) {
                        String containerString =
                                "\n\nContainer: " + key + " on " + thisNodeFile.getPath().getName();
                        System.out.println(containerString);
                        System.out.println(StringUtils.repeat("=", containerString.length()));
                        while (true) {
                            try {
                                AggregatedLogFormat.LogReader.readAContainerLogsForALogType(
                                        valueStream,
                                        System.out,
                                        thisNodeFile.getModificationTime());
                                foundAnyLogs = true;
                            } catch (EOFException eof) {
                                break;
                            }
                        }
                        // Next container
                        key = new AggregatedLogFormat.LogKey();
                        valueStream = reader.next(key);
                    }
                } finally {
                    reader.close();
                }
            }
        }
        if (!foundAnyLogs) {
            emptyLogDir(remoteAppLogDir.toString());
        }
    }

    public void printLogsByContainerId(
            JobParamsInfo jobParamsInfo, String applicationId, String containerId, String nodeId)
            throws IOException {
        YarnConfiguration configuration =
                YarnClusterClientFactory.INSTANCE.getYarnConf(jobParamsInfo.getYarnConfDir());

        Path remoteRootLogDir =
                new Path(
                        configuration.get(
                                YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
                                YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));
        ApplicationId appId = ConverterUtils.toApplicationId(applicationId);
        String user = UserGroupInformation.getCurrentUser().getShortUserName();
        String logDirSuffix = LogAggregationUtils.getRemoteNodeLogDirSuffix(configuration);

        Path remoteAppLogDir =
                LogAggregationUtils.getRemoteAppLogDir(remoteRootLogDir, appId, user, logDirSuffix);
        RemoteIterator<FileStatus> nodeFiles = null;
        try {
            Path qualifiedLogDir =
                    FileContext.getFileContext(configuration).makeQualified(remoteAppLogDir);
            nodeFiles =
                    FileContext.getFileContext(qualifiedLogDir.toUri(), configuration)
                            .listStatus(remoteAppLogDir);
        } catch (FileNotFoundException fnf) {
            logDirNotExist(remoteAppLogDir.toString());
        }

        boolean foundContainerLogs = false;
        while (nodeFiles.hasNext()) {
            FileStatus thisNodeFile = nodeFiles.next();
            String fileName = thisNodeFile.getPath().getName();
            if (fileName.contains(LogAggregationUtils.getNodeString(nodeId))
                    && !fileName.endsWith(LogAggregationUtils.TMP_FILE_SUFFIX)) {
                AggregatedLogFormat.LogReader reader = null;
                try {
                    reader =
                            new AggregatedLogFormat.LogReader(
                                    configuration, thisNodeFile.getPath());
                    if (dumpAContainerLogs(
                                    containerId,
                                    reader,
                                    System.out,
                                    thisNodeFile.getModificationTime())
                            > -1) {
                        foundContainerLogs = true;
                    }
                } finally {
                    if (reader != null) {
                        reader.close();
                    }
                }
            }
        }
        if (!foundContainerLogs) {
            containerLogNotFound(containerId);
        }
    }

    public int dumpAContainerLogs(
            String containerIdStr,
            AggregatedLogFormat.LogReader reader,
            PrintStream out,
            long logUploadedTime)
            throws IOException {
        DataInputStream valueStream;
        AggregatedLogFormat.LogKey key = new AggregatedLogFormat.LogKey();
        valueStream = reader.next(key);

        while (valueStream != null && !key.toString().equals(containerIdStr)) {
            // Next container
            key = new AggregatedLogFormat.LogKey();
            valueStream = reader.next(key);
        }

        if (valueStream == null) {
            return -1;
        }

        boolean foundContainerLogs = false;
        while (true) {
            try {
                AggregatedLogFormat.LogReader.readAContainerLogsForALogType(
                        valueStream, out, logUploadedTime);
                foundContainerLogs = true;
            } catch (EOFException eof) {
                break;
            }
        }
        if (foundContainerLogs) {
            return 0;
        }
        return -1;
    }

    private long getLogFileSize(Configuration yarnConfiguration, String tableLocation)
            throws IOException {
        Path inputPath = new Path(tableLocation);
        Configuration conf = new JobConf(yarnConfiguration);
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fsStatus = fs.listStatus(inputPath);

        long fileSize = Arrays.stream(fsStatus).mapToLong(FileStatus::getLen).sum();
        LOG.info("tableLocation:{} ,fileSize:{}", tableLocation, fileSize);
        return fileSize;
    }

    private static void logDirNotExist(String remoteAppLogDir) {
        System.out.println(remoteAppLogDir + " does not exist.");
        System.out.println("Log aggregation has not completed or is not enabled.");
    }

    private static void emptyLogDir(String remoteAppLogDir) {
        System.out.println(remoteAppLogDir + " does not have any log files.");
    }

    private static void containerLogNotFound(String containerId) {
        System.out.println(
                "Logs for container " + containerId + " are not present in this log-file.");
    }
}
