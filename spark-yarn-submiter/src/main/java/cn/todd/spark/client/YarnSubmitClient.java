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

package cn.todd.spark.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.ChildYarnClient;
import org.apache.spark.deploy.yarn.ClientArguments;
import scala.collection.mutable.HashMap;

import java.io.File;

/**
 *  为Spark任务绑定执行时使用的本地配置文件
 *
 */
public class YarnSubmitClient extends ChildYarnClient {
    public static String XML_SUFFIX = ".xml";
    public static String CONF_SUFFIX = ".conf";
    public String confLocalPath;


    public YarnSubmitClient(ClientArguments args, Configuration hadoopConf, SparkConf sparkConf, String confLocalPath) {
        super(args, hadoopConf, sparkConf);
        this.confLocalPath = confLocalPath;
    }

    @Override
    public void loadHadoopConf(scala.collection.mutable.HashMap hadoopConfFiles) {
        loadFromClasspath(hadoopConfFiles);
    }

    private void loadFromClasspath(HashMap hadoopConfFiles) {
        File confDir = new File(confLocalPath);
        File[] files = confDir.listFiles((dir, name) -> name.endsWith(XML_SUFFIX) || name.endsWith(CONF_SUFFIX));

        for (File file : files) {
            String fileName = file.getName();
            hadoopConfFiles.put(fileName, file);
        }
    }
}
