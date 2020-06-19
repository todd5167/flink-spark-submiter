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

    /** 可执行jar包执行参数 **/
    private String execArgs;

    /** hadoopConf  路径 **/
    private String hadoopConfDir;

    /** kubeConfig 本地路径 **/
    private String kubeConfig;

    /** 拉取的镜像名称 **/
    private String imageName;

    /**  spark 相关参数配置 **/
    private Properties confProperties;


    private JobParamsInfo(String appName, String mainClass, String runJarPath, String execArgs, String hadoopConfDir,
                          String kubeConfig, Properties confProperties, String imageName) {
        this.appName = appName;
        this.mainClass = mainClass;
        this.runJarPath = runJarPath;
        this.kubeConfig = kubeConfig;
        this.execArgs = execArgs;
        this.hadoopConfDir = hadoopConfDir;
        this.kubeConfig = kubeConfig;
        this.confProperties = confProperties;
        this.imageName = imageName;
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

    public String getExecArgs() {
        return execArgs;
    }

    public String getHadoopConfDir() {
        return hadoopConfDir;
    }

    public String getKubeConfig() {
        return kubeConfig;
    }

    public Properties getConfProperties() {
        return confProperties;
    }

    public String getImageName() {
        return imageName;
    }

    public static Builder builder() {
        return new Builder();
    }


    public static class Builder {
        private String appName;
        private String mainClass;
        private String runJarPath;
        private String execArgs;
        private String hadoopConfDir;
        private String kubeConfig;
        private String imageName;
        private Properties confProperties;


        public Builder setAppName(String appName) {
            this.appName = appName;
            return this;
        }

        public Builder setMainClass(String mainClass) {
            this.mainClass = mainClass;
            return this;
        }

        public Builder setRunJarPath(String runJarPath) {
            this.runJarPath = runJarPath;
            return this;
        }

        public Builder setExecArgs(String execArgs) {
            this.execArgs = execArgs;
            return this;
        }

        public Builder setHadoopConfDir(String hadoopConfDir) {
            this.hadoopConfDir = hadoopConfDir;
            return this;
        }

        public Builder setKubeConfig(String kubeConfig) {
            this.kubeConfig = kubeConfig;
            return this;
        }

        public Builder setConfProperties(Properties confProperties) {
            this.confProperties = confProperties;
            return this;
        }

        public Builder setImageName(String imageName) {
            this.imageName = imageName;
            return this;
        }

        public JobParamsInfo build() {
            return new JobParamsInfo(appName, mainClass, runJarPath, execArgs, hadoopConfDir, kubeConfig, confProperties,imageName);
        }
    }

}
