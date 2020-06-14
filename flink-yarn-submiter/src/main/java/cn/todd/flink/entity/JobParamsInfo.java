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

package cn.todd.flink.entity;

import java.util.Properties;

/**
 *
 *  任务执行依赖的参数配置
 *  Date: 2020/6/14
 *
 * @author todd5167
 */
public class JobParamsInfo {

    private String name;
    private String queue;
    private String runJarPath;
    private String flinkConfDir;
    private String flinkJarPath;
    private String yarnConfDir;
    private String[] dependFile;
    private String[] execArgs;
    private Properties confProperties;
    private Properties yarnSessionConfProperties;

    private JobParamsInfo(String name, String queue, String runJarPath, String flinkConfDir, String yarnConfDir,
                          String[] execArgs, Properties confProperties, Properties yarnSessionConfProperties,
                          String[] dependFile, String flinkJarPath) {
        this.name = name;
        this.queue = queue;
        this.runJarPath = runJarPath;
        this.flinkConfDir = flinkConfDir;
        this.yarnConfDir = yarnConfDir;
        this.execArgs = execArgs;
        this.confProperties = confProperties;
        this.yarnSessionConfProperties = yarnSessionConfProperties;
        this.dependFile = dependFile;
        this.flinkJarPath = flinkJarPath;
    }

    public String getName() {
        return name;
    }

    public String getQueue() {
        return queue;
    }

    public String getFlinkConfDir() {
        return flinkConfDir;
    }

    public String getYarnConfDir() {
        return yarnConfDir;
    }


    public String[] getExecArgs() {
        return execArgs;
    }

    public Properties getConfProperties() {
        return confProperties;
    }

    public Properties getYarnSessionConfProperties() {
        return yarnSessionConfProperties;
    }

    public String getRunJarPath() {
        return runJarPath;
    }

    public String[] getDependFile() {
        return dependFile;
    }

    public String getFlinkJarPath() {
        return flinkJarPath;
    }

    public static JobParamsInfo.Builder builder() {
        return new JobParamsInfo.Builder();
    }


    public static class Builder {
        private String name;
        private String queue;
        private String runJarPath;
        private String flinkConfDir;
        private String flinkJarPath;
        private String yarnConfDir;
        private String[] dependFile;
        private String[] execArgs;
        private Properties confProperties;
        private Properties yarnSessionConfProperties;

        public JobParamsInfo.Builder setName(String name) {
            this.name = name;
            return this;
        }

        public JobParamsInfo.Builder setQueue(String queue) {
            this.queue = queue;
            return this;
        }

        public JobParamsInfo.Builder setFlinkConfDir(String flinkConfDir) {
            this.flinkConfDir = flinkConfDir;
            return this;
        }

        public JobParamsInfo.Builder setYarnConfDir(String yarnConfDir) {
            this.yarnConfDir = yarnConfDir;
            return this;
        }

        public JobParamsInfo.Builder setExecArgs(String[] execArgs) {
            this.execArgs = execArgs;
            return this;
        }

        public JobParamsInfo.Builder setConfProperties(Properties confProperties) {
            this.confProperties = confProperties;
            return this;
        }

        public JobParamsInfo.Builder setYarnSessionConfProperties(Properties yarnSessionConfProperties) {
            this.yarnSessionConfProperties = yarnSessionConfProperties;
            return this;
        }


        public JobParamsInfo.Builder setFlinkJarPath(String flinkJarPath) {
            this.flinkJarPath = flinkJarPath;
            return this;
        }

        public JobParamsInfo.Builder setRunJarPath(String runJarPath) {
            this.runJarPath = runJarPath;
            return this;
        }

        public JobParamsInfo.Builder setDependFile(String[] dependFile) {
            this.dependFile = dependFile;
            return this;
        }

        public JobParamsInfo build() {
            return new JobParamsInfo(name, queue, runJarPath, flinkConfDir, yarnConfDir, execArgs,
                    confProperties, yarnSessionConfProperties, dependFile, flinkJarPath);
        }
    }

}
