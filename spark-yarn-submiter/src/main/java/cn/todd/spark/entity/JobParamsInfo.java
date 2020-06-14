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

package cn.todd.spark.entity;

import java.util.Properties;

/**
 *
 *  任务执行依赖的参数配置
 *  Date: 2020/6/14
 *
 * @author todd5167
 */
public class JobParamsInfo {
    /**  应用名称**/
    private String appName;

    /** Main类 **/
    private String mainClass;

    /** 可执行jar包本地路径 **/
    private String runJarPath;

    /** 可执行jar上传到Hdfs的路径 **/
    private String jarHdfsDir;

    /** 可执行jar包执行参数 **/
    private String execArgs;

    /** archive hdfs路径 **/
    private String archivePath;

    /** yarnConf  路径 **/
    private String yarnConfDir;

    /** yarn 队列名称 **/
    private String queue;

    /**  spark 相关参数配置 **/
    private Properties confProperties;

    private String principal;
    private String keytab;

    private String openKerberos;


    private JobParamsInfo(String appName, String mainClass, String runJarPath, String jarHdfsDir, String execArgs, String openKerberos,
                          String archivePath, String yarnConfDir, String queue, Properties confProperties, String principal, String keytab) {
        this.appName = appName;
        this.mainClass = mainClass;
        this.runJarPath = runJarPath;
        this.queue = queue;
        this.jarHdfsDir = jarHdfsDir;
        this.archivePath = archivePath;

        this.yarnConfDir = yarnConfDir;
        this.execArgs = execArgs;
        this.confProperties = confProperties;

        this.principal = principal;
        this.keytab = keytab;
        this.openKerberos = openKerberos;
    }

    public String getAppName() {
        return appName;
    }

    public String getMainClass() {
        return mainClass;
    }

    public String getRunJarPath() {
        return runJarPath;
    }

    public String getJarHdfsDir() {
        return jarHdfsDir;
    }

    public String getExecArgs() {
        return execArgs;
    }

    public String getArchivePath() {
        return archivePath;
    }

    public String getYarnConfDir() {
        return yarnConfDir;
    }

    public String getQueue() {
        return queue;
    }

    public Properties getConfProperties() {
        return confProperties;
    }

    public String getPrincipal() {
        return principal;
    }

    public String getKeytab() {
        return keytab;
    }

    public String getOpenKerberos() {
        return openKerberos;
    }

    public static JobParamsInfo.Builder builder() {
        return new JobParamsInfo.Builder();
    }


    public static class Builder {
        private String appName;
        private String mainClass;
        private String runJarPath;
        private String jarHdfsDir;
        private String execArgs;
        private String archivePath;
        private String yarnConfDir;
        private String queue;
        private String principal;
        private String keytab;
        private String openKerberos;
        private Properties confProperties;

        public JobParamsInfo.Builder setAppName(String appName) {
            this.appName = appName;
            return this;
        }

        public JobParamsInfo.Builder setMainClass(String mainClass) {
            this.mainClass = mainClass;
            return this;
        }

        public JobParamsInfo.Builder setJarHdfsDir(String jarHdfsDir) {
            this.jarHdfsDir = jarHdfsDir;
            return this;
        }

        public JobParamsInfo.Builder setArchivePath(String archivePath) {
            this.archivePath = archivePath;
            return this;
        }

        public JobParamsInfo.Builder setPrincipal(String principal) {
            this.principal = principal;
            return this;
        }

        public JobParamsInfo.Builder setKeytab(String keytab) {
            this.keytab = keytab;
            return this;
        }

        public JobParamsInfo.Builder setQueue(String queue) {
            this.queue = queue;
            return this;
        }

        public JobParamsInfo.Builder setYarnConfDir(String yarnConfDir) {
            this.yarnConfDir = yarnConfDir;
            return this;
        }

        public JobParamsInfo.Builder setExecArgs(String execArgs) {
            this.execArgs = execArgs;
            return this;
        }

        public JobParamsInfo.Builder setConfProperties(Properties confProperties) {
            this.confProperties = confProperties;
            return this;
        }

        public JobParamsInfo.Builder setRunJarPath(String runJarPath) {
            this.runJarPath = runJarPath;
            return this;
        }

        public JobParamsInfo.Builder setOpenKerberos(String openKerberos) {
            this.openKerberos = openKerberos;
            return this;
        }

        public JobParamsInfo build() {
            return new JobParamsInfo(appName, mainClass, runJarPath, jarHdfsDir, execArgs, openKerberos,
                    archivePath, yarnConfDir, queue, confProperties, principal, keytab);
        }
    }

}
