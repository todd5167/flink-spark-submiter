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


package cn.todd.flink.launcher;

import cn.todd.flink.entity.JobParamsInfo;
import cn.todd.flink.enums.ERunMode;
import cn.todd.flink.enums.ETaskStatus;
import cn.todd.flink.executor.StandaloneExecutor;
import cn.todd.flink.executor.YarnJobClusterExecutor;
import cn.todd.flink.executor.YarnSessionClusterExecutor;
import cn.todd.flink.log.FinishedLog;
import cn.todd.flink.log.RunningLog;
import org.apache.commons.math3.util.Pair;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 *
 *  Flink任务本地idea提交
 *  Date: 2020/6/14
 *
 * @author todd5167
 */

public class LauncherMain {

    public static Optional<Pair<String, String>> submitFlinkJob(JobParamsInfo jobParamsInfo) throws Exception {
        Optional<Pair<String, String>> appIdAndJobId = Optional.empty();
        ERunMode runMode = ERunMode.convertFromString(jobParamsInfo.getRunMode());
        switch (runMode) {
            case YARN_SESSION:
                appIdAndJobId = new YarnSessionClusterExecutor(jobParamsInfo).submit();
                break;
            case YARN_PERJOB:
                appIdAndJobId = new YarnJobClusterExecutor(jobParamsInfo).submit();
                break;
            case STANDALONE:
                new StandaloneExecutor(jobParamsInfo).submit();
                break;
            default:
                throw new RuntimeException("Unsupported operating mode, support YARN_SESSION,YARN_SESSION,STANDALONE");
        }
        return appIdAndJobId;
    }

    public static void cancelFlinkJob(JobParamsInfo jobParamsInfo, Pair<String, String> appIdAndJobId) throws Exception {
        String yarnApplicationId = appIdAndJobId.getFirst();
        String jobId = appIdAndJobId.getSecond();

        Preconditions.checkNotNull(yarnApplicationId, "application id not null!");
        Preconditions.checkNotNull(jobId, "job  id not null!");

        ERunMode runMode = ERunMode.convertFromString(jobParamsInfo.getRunMode());
        switch (runMode) {
            case YARN_SESSION:
                new YarnSessionClusterExecutor(jobParamsInfo).cancel(yarnApplicationId, jobId);
                break;
            case YARN_PERJOB:
                new YarnJobClusterExecutor(jobParamsInfo).cancel(yarnApplicationId, jobId);
                break;
            case STANDALONE:
                new StandaloneExecutor(jobParamsInfo).cancel(yarnApplicationId, jobId);
            default:
                throw new RuntimeException("Unsupported operating mode, yarnSession,yarnPer");
        }
    }
    //todo test STANDALONE
    public static ETaskStatus getJobStatus(JobParamsInfo jobParamsInfo, Pair<String, String> appIdAndJobId) throws Exception {
        String yarnApplicationId = appIdAndJobId.getFirst();
        String jobId = appIdAndJobId.getSecond();

        ERunMode runMode = ERunMode.convertFromString(jobParamsInfo.getRunMode());
        ETaskStatus jobStatus;
        switch (runMode) {
            case YARN_SESSION:
                jobStatus = new YarnSessionClusterExecutor(jobParamsInfo).getJobStatus(yarnApplicationId, jobId);
                break;
            case YARN_PERJOB:
                jobStatus = new YarnJobClusterExecutor(jobParamsInfo).getJobStatus(yarnApplicationId, jobId);
                break;
            case STANDALONE:
                jobStatus = new StandaloneExecutor(jobParamsInfo).getJobStatus(yarnApplicationId, jobId);
            default:
                throw new RuntimeException("Unsupported operating mode, yarnSession,yarnPer");
        }
        return jobStatus;
    }

    public static void printRollingLogBaseInfo(JobParamsInfo jobParamsInfo, Pair<String, String> appIdAndJobId) {
        try {
            //获取运行日志
            Thread.sleep(20000);
            List<String> logsInfo = new RunningLog().getRollingLogBaseInfo(jobParamsInfo, appIdAndJobId.getFirst());
            logsInfo.forEach(System.out::println);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *  yarn per job fininshed log
     * @param jobParamsInfo
     * @param applicationId
     */
    public static void printFinishedLog(JobParamsInfo jobParamsInfo, String applicationId) {
        try {
            new FinishedLog().printAllContainersLogs(jobParamsInfo, applicationId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static JobParamsInfo buildJobParamsInfo() {

        //        System.setProperty("java.security.krb5.conf", "/Users/maqi/tmp/hadoopconf/cdh514/krb5.conf");
        // 可执行jar包路径
        String runJarPath = "/Users/maqi/code/ClustersSubmiter/exampleJars/flink-kafka-reader/flink-kafka-reader.jar";
        // 任务参数
        String[] execArgs = new String[]{"-jobName", "flink110Submit", "--topic", "mqTest01", "--bootstrapServers", "172.16.8.107:9092"};
        // 任务名称
        String jobName = "Flink perjob submit";
        // flink 文件夹路径
        String flinkConfDir = "/Users/maqi/tmp/flink/flink-1.10.0/conf";
        // flink lib包路径
        String flinkJarPath = "/Users/maqi/tmp/flink/flink-1.10.0/lib";
        //  yarn 文件夹路径
        String yarnConfDir = "/Users/maqi/tmp/hadoopconf/dev40/hadoop";
        // perjob 运行流任务
        String runMode = "yarn_perjob";
        //  作业依赖的外部文件
        String[] dependFile = new String[]{"/Users/maqi/tmp/flink/flink-1.10.0/README.txt"};
        // 任务提交队列
        String queue = "default";
        // yarnsession appid配置
        Properties yarnSessionConfProperties = new Properties();
        yarnSessionConfProperties.setProperty("yid", "application_1594265598097_5425");

        // 非必要参数，可以通过shade打包指定mainClass, flink自动获取
        // String entryPointClassName = "cn.todd.flink.KafkaReader";
        String entryPointClassName = null;

        // savepoint 及并行度相关
        Properties confProperties = new Properties();
        confProperties.setProperty("parallelism", "1");


        JobParamsInfo jobParamsInfo = JobParamsInfo.builder()
                .setExecArgs(execArgs)
                .setName(jobName)
                .setRunJarPath(runJarPath)
                .setDependFile(dependFile)
                .setFlinkConfDir(flinkConfDir)
                .setYarnConfDir(yarnConfDir)
                .setConfProperties(confProperties)
                .setYarnSessionConfProperties(yarnSessionConfProperties)
                .setFlinkJarPath(flinkJarPath)
                .setQueue(queue)
                .setRunMode(runMode)
                .setEntryPointClassName(entryPointClassName)
                .build();

        return jobParamsInfo;
    }


    public static void main(String[] args) throws Exception {
        JobParamsInfo jobParamsInfo = buildJobParamsInfo();
        Optional<Pair<String, String>> appIdAndJobId = submitFlinkJob(jobParamsInfo);

//        // running log info
//        appIdAndJobId.ifPresent((pair) -> printRollingLogBaseInfo(jobParamsInfo, pair));
//
//        // cancel job
//        Pair<String, String> job = new Pair<>("application_1594265598097_2688", "35a679c9f94311a8a8084e4d8d06a95d");
//        cancelFlinkJob(jobParamsInfo, job);
//
//
//        // getJobStatus
//        ETaskStatus jobStatus = getJobStatus(jobParamsInfo, new Pair<>("application_1594265598097_5425", "fa4ae50441c5d5363e8abbe5623e115a"));
//        System.out.println("job status is : " + jobStatus.toString());
//
//        // print finished Log
//        printFinishedLog(jobParamsInfo,"application_1594961717891_0103");
    }
}
