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

import cn.todd.common.utils.HadoopConfParseUtil;
import cn.todd.common.utils.PublicUtil;
import cn.todd.spark.entity.JobParamsInfo;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.k8s.ExtendConfig;
import org.apache.spark.deploy.k8s.submit.ClientArguments;
import org.apache.spark.deploy.k8s.submit.ChildKubernetesClientApplication;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;


public class LauncherMain {
    public static String run(JobParamsInfo jobParamsInfo) {

        List<String> argList = new ArrayList<>();
        argList.add("--main-class");
        argList.add(jobParamsInfo.getMainClass());
        argList.add("--primary-java-resource");
        argList.add(jobParamsInfo.getRunJarPath());
        argList.add("--arg");
        argList.add(jobParamsInfo.getExecArgs());

        ChildKubernetesClientApplication k8sApp = new ChildKubernetesClientApplication();
        ClientArguments clientArguments = ClientArguments.fromCommandLineArgs(argList.toArray(new String[argList.size()]));
        SparkConf sparkConf = buildSparkConf(jobParamsInfo);
        String driverSelectorId = k8sApp.run(clientArguments, sparkConf);
        return driverSelectorId;
    }

    private static SparkConf buildSparkConf(JobParamsInfo jobParamsInfo) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName(jobParamsInfo.getAppName());

        sparkConf.set("deploy-mode", "cluster");
        sparkConf.set(ExtendConfig.KUBERNETES_KUBE_CONFIG_KEY(), jobParamsInfo.getKubeConfig());
        sparkConf.set("spark.kubernetes.container.image", jobParamsInfo.getImageName());
        sparkConf.set("spark.kubernetes.submission.waitAppCompletion", "false");

        String hadoopConfDir = jobParamsInfo.getHadoopConfDir();
        Optional.ofNullable(hadoopConfDir)
                .ifPresent((confDir) -> HadoopConfParseUtil.getHadoopConf(confDir).forEach(sparkConf::set));

        Properties confProperties = jobParamsInfo.getConfProperties();
        confProperties.
                stringPropertyNames()
                .stream()
                .filter(name -> StringUtils.startsWith(name, "spark."))
                .forEach(name -> sparkConf.set(name, confProperties.getProperty(name)));

        return sparkConf;
    }

    /**
     *  spark sql proxy jar params
     * @return
     * @throws IOException
     */
    private static String getExampleJobParams() throws IOException {
        String sql = "use yuebai; " +
                "INSERT INTO mq_table_textfile_0031 PARTITION(pt = '20200611')\n" +
                "VALUES (1008,'3m2221','m2232','n333','maqq33qqq','maqqqqq','maqqqqq','maqqqqq');";

        Map<String, Object> paramsMap = new HashMap<>();
        paramsMap.put("sql", URLEncoder.encode(sql, Charsets.UTF_8.name()));
        paramsMap.put("appName", "toddSparkSubmit");
        paramsMap.put("sparkSessionConf", getSparkSessionConf());

        String sqlExeJson = null;
        sqlExeJson = PublicUtil.objToString(paramsMap);
        sqlExeJson = URLEncoder.encode(sqlExeJson, Charsets.UTF_8.name());

        return sqlExeJson;
    }

    private static Map<String, String> getSparkSessionConf() {
        Map<String, String> map = Maps.newHashMap();
        map.put("hive.default.fileformat", "orc");
        return map;
    }


    public static void main(String[] args) throws Exception {
        String appName = "todd spark submit";
        // 镜像内的jar路径
        String runJarPath = "local:///opt/dtstack/spark/spark-sql-proxy.jar";
        String mainClass = "cn.todd.spark.SparksqlProxy";
        String hadoopConfDir = "/Users/maqi/tmp/hadoopconf/dev40/hadoop";
        String kubeConfig = "/Users/maqi/tmp/flink/flink-1.10.0/conf/k8s.config";
        String imageName = "mqspark:2.4.8";
        String execArgs = getExampleJobParams();

        Properties confProperties = new Properties();
        confProperties.setProperty("spark.executor.instances", "2");
        confProperties.setProperty("spark.kubernetes.namespace", "default");
        confProperties.setProperty("spark.kubernetes.authenticate.driver.serviceAccountName", "spark");
        confProperties.setProperty("spark.kubernetes.container.image.pullPolicy", "IfNotPresent");


        JobParamsInfo jobParamsInfo = JobParamsInfo.builder()
                .setAppName(appName)
                .setRunJarPath(runJarPath)
                .setMainClass(mainClass)
                .setExecArgs(execArgs)
                .setConfProperties(confProperties)
                .setHadoopConfDir(hadoopConfDir)
                .setKubeConfig(kubeConfig)
                .setImageName(imageName)
                .build();

        String id = run(jobParamsInfo);
        System.out.println(id);
    }


}
