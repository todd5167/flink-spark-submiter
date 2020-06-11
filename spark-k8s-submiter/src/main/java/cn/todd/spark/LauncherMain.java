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

package cn.todd.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.deploy.k8s.submit.ClientArguments;
import org.apache.spark.deploy.k8s.submit.ChildKubernetesClientApplication;

import java.util.ArrayList;
import java.util.List;


public class LauncherMain {
    public static void main(String[] args) {

        List<String> argList = new ArrayList<>();
        argList.add("--main-class");
        argList.add("cn.todd.spark.proxy.SparkWordCount");
        argList.add("--primary-java-resource");
        argList.add("local:///opt/dtstack/spark/spark-proxy-1.0.0.jar");


        ChildKubernetesClientApplication k8sApp = new ChildKubernetesClientApplication();
        ClientArguments clientArguments = ClientArguments.fromCommandLineArgs(argList.toArray(new String[argList.size()]));

        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.executor.instances", "2");
        sparkConf.set("deploy-mode", "cluster");
        sparkConf.set("spark.kubernetes.namespace", "default");
        sparkConf.set("spark.kubernetes.kubeConfig", "/Users/maqi/tmp/flink/flink-1.10.0/conf/k8s.config");
        sparkConf.set("spark.kubernetes.container.image", "mqspark:2.4.4");
        sparkConf.set("spark.kubernetes.authenticate.driver.serviceAccountName", "spark");
        sparkConf.set("spark.kubernetes.container.image.pullPolicy", "IfNotPresent");
        sparkConf.set("spark.kubernetes.submission.waitAppCompletion", "false");

        sparkConf.setAppName("Spark k8s submit test");

        String appId = k8sApp.run(clientArguments, sparkConf);

        System.out.println(appId);
    }


}
