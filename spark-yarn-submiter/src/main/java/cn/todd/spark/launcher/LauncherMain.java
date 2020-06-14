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

package cn.todd.spark.launcher;

import cn.todd.spark.client.YarnSubmitClient;
import cn.todd.spark.utils.PublicUtil;
import cn.todd.spark.utils.StringUtil;
import cn.todd.spark.utils.YarnConfLoaderUtil;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.ClientArguments;

import java.net.URLEncoder;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *  Spark 任务提交YARN
 */
public class LauncherMain {


    public static String run(String yarnConfDir, YarnConfiguration yarnConf) throws Exception {

        String sql = "use sparktest;select * from mq_table_parquet_001;";

        Map<String, Object> paramsMap = new HashMap<>();
        String zipSql = StringUtil.zip(sql);

        paramsMap.put("sql", zipSql);
        paramsMap.put("appName", "toddSparkSubmit");
        paramsMap.put("sparkSessionConf", getSparkSessionConf());


        String sqlExeJson = null;
        sqlExeJson = PublicUtil.objToString(paramsMap);
        sqlExeJson = URLEncoder.encode(sqlExeJson, Charsets.UTF_8.name());


        List<String> argList = new ArrayList<>();
        argList.add("--jar");
        argList.add("hdfs://nameservice1/sparkproxy/sql-proxy.jar");
        argList.add("--class");
        argList.add("com.dtstack.sql.main.SqlProxy");
        argList.add("--arg");
        argList.add(sqlExeJson);


        ClientArguments clientArguments = new ClientArguments(argList.toArray(new String[argList.size()]));
        SparkConf sparkConf = buildSparkConf();

        YarnSubmitClient clientExt = new YarnSubmitClient(clientArguments, yarnConf, sparkConf, yarnConfDir);
        ApplicationId applicationId = clientExt.submitApplication(0);
        return applicationId.toString();
    }

    public static void main(String[] args) throws Exception {
        boolean openKerberos = true;
        String yarnConfDir = "/Users/maqi/tmp/__spark_conf__6181052549606078780";
        YarnConfiguration yarnConf = YarnConfLoaderUtil.getYarnConf(yarnConfDir);
        String applicationId = "";

        if (openKerberos) {
            String principal = "hdfs/node1@DTSTACK.COM";
            String keyTab = "/Users/maqi/tmp/hadoopconf/cdh514/hdfs.keytab";

            System.setProperty("java.security.krb5.conf", "/Users/maqi/tmp/hadoopconf/cdh514/krb5.conf");

            UserGroupInformation.setConfiguration(yarnConf);
            UserGroupInformation userGroupInformation = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keyTab);


            applicationId = userGroupInformation.doAs(new PrivilegedAction<String>() {
                @Override
                public String run() {
                    try {
                        return LauncherMain.run(yarnConfDir, yarnConf);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return "";
                }
            });
        } else {
            LauncherMain.run(yarnConfDir, yarnConf);
        }

        System.out.println(applicationId);
    }

    private static Map<String, String> getSparkSessionConf() {
        Map<String, String> map = Maps.newHashMap();
        map.put("hive.default.fileformat", "orc");
        return map;
    }

    private static SparkConf buildSparkConf() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.remove("spark.jars");
        sparkConf.remove("spark.files");

        sparkConf.set("spark.yarn.archive", "hdfs://nameservice1/sparkjars/jars");
        sparkConf.set("spark.yarn.queue", "root.users.hdfs");
        sparkConf.set("spark.submit.deployMode", "cluster");

        sparkConf.set("spark.yarn.keytab", "/Users/maqi/tmp/hadoopconf/cdh514/hdfs.keytab");
        sparkConf.set("spark.yarn.principal", "hdfs/node1@DTSTACK.COM");
        sparkConf.set("security", "true");

        sparkConf.setAppName("todd spark submit");
        return sparkConf;
    }
}
