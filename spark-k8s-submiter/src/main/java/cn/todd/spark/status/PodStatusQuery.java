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

package cn.todd.spark.status;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;


public class PodStatusQuery {
    public static void main(String[] args) throws FileNotFoundException {
        String configPath = "/Users/maqi/tmp/flink/flink-1.10.0/conf/k8s.config";
        Config config = Config.fromKubeconfig(getContentFromFile(configPath));
        config.setNamespace("default");

        KubernetesClient client = new DefaultKubernetesClient(config);


        List<Pod> items = client.pods()
                .withLabel("spark-app-selector", "spark-d2704592c83b4b3bb2a337c88c4b0a73")
                .list()
                .getItems();

        if (items.size() == 0) {
            System.out.println("Pod已删除");
            return;
        }

        String phase = items.get(0).getStatus().getPhase();
        System.out.println("当前状态：" + phase);

    }

    public static String getContentFromFile(String filePath) throws FileNotFoundException {
        File file = new File(filePath);
        if (file.exists()) {
            StringBuilder content = new StringBuilder();
            String line;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
                while ((line = reader.readLine()) != null) {
                    content.append(line).append(System.lineSeparator());
                }
            } catch (IOException e) {
                throw new RuntimeException("Error read file content.", e);
            }
            return content.toString();
        }
        throw new FileNotFoundException("File " + filePath + " not exists.");
    }

}
