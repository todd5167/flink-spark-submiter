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
import cn.todd.spark.entity.JobParamsInfo;
import cn.todd.spark.utils.HdfsUtil;
import cn.todd.spark.utils.PublicUtil;
import cn.todd.spark.utils.TdStringUtil;
import cn.todd.spark.utils.YarnConfLoaderUtil;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import jodd.util.StringUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.ClientArguments;

import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 *  Spark 任务提交YARN
 */
public class LauncherMain {

    public static String run(JobParamsInfo jobParamsInfo, YarnConfiguration yarnConf) throws Exception {
        String jarHdfsPath = uploadJarToHdfs(yarnConf, jobParamsInfo);
        if (StringUtil.isEmpty(jarHdfsPath)) {
            throw new RuntimeException("jar hdfs path not null!");
        }

        List<String> argList = new ArrayList<>();
        argList.add("--jar");
        argList.add(jarHdfsPath);
        argList.add("--class");
        argList.add(jobParamsInfo.getMainClass());
        argList.add("--arg");
        argList.add(jobParamsInfo.getExecArgs());

        ClientArguments clientArguments = new ClientArguments(argList.toArray(new String[argList.size()]));
        SparkConf sparkConf = buildSparkConf(jobParamsInfo);

        YarnSubmitClient clientExt = new YarnSubmitClient(clientArguments, yarnConf, sparkConf, jobParamsInfo.getYarnConfDir());
        ApplicationId applicationId = clientExt.submitApplication(0);
        return applicationId.toString();
    }

    private static SparkConf buildSparkConf(JobParamsInfo jobParamsInfo) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.submit.deployMode", "cluster");
        sparkConf.set("spark.yarn.archive", jobParamsInfo.getArchivePath());
        sparkConf.set("spark.yarn.queue", jobParamsInfo.getQueue());
        sparkConf.set("spark.yarn.keytab", jobParamsInfo.getKeytab());
        sparkConf.set("spark.yarn.principal", jobParamsInfo.getPrincipal());
        sparkConf.set("security", jobParamsInfo.getOpenKerberos());

        Properties confProperties = jobParamsInfo.getConfProperties();
        confProperties.
                stringPropertyNames()
                .stream()
                .filter(name -> StringUtils.startsWith(name, "spark."))
                .forEach(name -> sparkConf.set(name, confProperties.getProperty(name)));

        sparkConf.setAppName(jobParamsInfo.getAppName());
        return sparkConf;
    }

    /**
     *  将Jar上传到Hdfs上，返回唯一路径
     * @param yarnConf
     * @param jobParamsInfo
     * @return
     * @throws IOException
     */
    private static String uploadJarToHdfs(YarnConfiguration yarnConf, JobParamsInfo jobParamsInfo) throws IOException {
        String jarHdfsDir = jobParamsInfo.getJarHdfsDir();
        String runJarPath = jobParamsInfo.getRunJarPath();

        Preconditions.checkNotNull(jarHdfsDir, "jar Hdfs Dir not null!");
        Preconditions.checkState(FileUtils.getFile(runJarPath).exists() && FileUtils.getFile(runJarPath).isFile(), "run jar path  error");

        HdfsUtil.mkdir(yarnConf, jarHdfsDir);
        return HdfsUtil.uploadFile(yarnConf, runJarPath, jarHdfsDir);
    }

    //TODO  ADD
    private static Map<String, String> getSparkSessionConf() {
        Map<String, String> map = Maps.newHashMap();
        map.put("hive.default.fileformat", "orc");
        return map;
    }

    /**
     *  spark sql proxy jar params
     * @return
     * @throws IOException
     */
    private static String getExampleJobParams() throws IOException {
        String sql = "use sparktest;select * from mq_table_parquet_001;";
        Map<String, Object> paramsMap = new HashMap<>();

        paramsMap.put("sql", URLEncoder.encode(sql, Charsets.UTF_8.name()));
        paramsMap.put("appName", "toddSparkSubmit");
        paramsMap.put("sparkSessionConf", getSparkSessionConf());

        String sqlExeJson = null;
        sqlExeJson = PublicUtil.objToString(paramsMap);
        sqlExeJson = URLEncoder.encode(sqlExeJson, Charsets.UTF_8.name());

        return sqlExeJson;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("java.security.krb5.conf", "/Users/maqi/tmp/hadoopconf/cdh514/krb5.conf");

        boolean openKerberos = true;
        String appName = "todd spark submit";
        String runJarPath = "/Users/maqi/code/ClustersSubmiter/exampleJars/spark-sql-proxy/spark-sql-proxy.jar";
        String mainClass = "cn.todd.spark.SparksqlProxy";
        String yarnConfDir = "/Users/maqi/tmp/__spark_conf__6181052549606078780";
        String principal = "hdfs/node1@DTSTACK.COM";
        String keyTab = "/Users/maqi/tmp/hadoopconf/cdh514/hdfs.keytab";
        String jarHdfsDir = "sparkproxy2";
        String archive = "hdfs://nameservice1/sparkjars/jars";
        String queue = "root.users.hdfs";
        String execArgs = getExampleJobParams();

        Properties confProperties = new Properties();
        confProperties.setProperty("spark.executor.cores","2");

        JobParamsInfo jobParamsInfo = JobParamsInfo.builder()
                .setAppName(appName)
                .setRunJarPath(runJarPath)
                .setMainClass(mainClass)
                .setYarnConfDir(yarnConfDir)
                .setPrincipal(principal)
                .setKeytab(keyTab)
                .setJarHdfsDir(jarHdfsDir)
                .setArchivePath(archive)
                .setQueue(queue)
                .setExecArgs(execArgs)
                .setConfProperties(confProperties)
                .setOpenKerberos(BooleanUtils.toString(openKerberos, "true", "false"))
                .build();

        YarnConfiguration yarnConf = YarnConfLoaderUtil.getYarnConf(yarnConfDir);
        String applicationId = "";

        if (BooleanUtils.toBoolean(openKerberos)) {
            UserGroupInformation.setConfiguration(yarnConf);
            UserGroupInformation userGroupInformation = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keyTab);
            applicationId = userGroupInformation.doAs((PrivilegedExceptionAction<String>) () -> LauncherMain.run(jobParamsInfo, yarnConf));
        } else {
            LauncherMain.run(jobParamsInfo, yarnConf);
        }

        System.out.println(applicationId);
    }
}
