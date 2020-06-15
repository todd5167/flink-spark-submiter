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

package cn.todd.flink.log;

import cn.todd.flink.entity.JobParamsInfo;
import cn.todd.flink.factory.YarnClusterClientFactory;
import cn.todd.flink.utils.ApplicationWSParser;
import cn.todd.flink.utils.HttpClientUtil;
import cn.todd.flink.utils.UrlUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.util.function.FunctionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 *  Flink任务获取执行日志基本信息
 *
 * Date: 2020/6/15
 * @author todd5167
 */
public class RunningLog {
    private static final Logger LOG = LoggerFactory.getLogger(RunningLog.class);

    private static final String APPLICATION_REST_API_TMP = "%s/ws/v1/cluster/apps/%s";

    public List<String> getRollingLogBaseInfo(JobParamsInfo jobParamsInfo, String applicatonId) throws Exception {
        ApplicationId applicationId = ConverterUtils.toApplicationId(applicatonId);
        YarnConfiguration yarnConf = YarnClusterClientFactory.INSTANCE.getYarnConf(jobParamsInfo.getYarnConfDir());

        YarnClient yarnClient = buildYarnClient(yarnConf);
        ApplicationReport report = yarnClient.getApplicationReport(applicationId);

        String rmUrl = StringUtils.substringBefore(report.getTrackingUrl().split("//")[1], "/");

        String amRootURl = String.format(APPLICATION_REST_API_TMP, UrlUtil.getHttpRootURL(rmUrl), applicatonId);
        List<String> result = Lists.newArrayList();
        try {
            String response = HttpClientUtil.getRequest(amRootURl);
            if (!StringUtils.isEmpty(response)) {
                ApplicationWSParser applicationWSParser = new ApplicationWSParser(response);
                String amStatue = applicationWSParser.getParamContent(ApplicationWSParser.AM_STATUE);

                if (!StringUtils.equalsIgnoreCase(amStatue, "RUNNING")) {
                    return result;
                }

                //解析AM 日志基本信息
                String amContainerLogsURL = applicationWSParser.getParamContent(ApplicationWSParser.AM_CONTAINER_LOGS_TAG);
                String logPreURL = UrlUtil.getHttpRootURL(amContainerLogsURL);
                String amLogInfo = applicationWSParser.parseContainerLogBaseInfo(amContainerLogsURL, logPreURL);

                result.add(amLogInfo);


                // 通过获取Flink任务包含的taskmanagers，拿到对应的Containerid
                String trackingUrl = applicationWSParser.getParamContent(ApplicationWSParser.TRACKING_URL);
                String taskManagerUrl = trackingUrl + "/taskmanagers";
                List<String> containerIds = getContainersId(taskManagerUrl);

                if (containerIds.size() > 0) {
                    // 将amContainerLogs替换为taskManager对应的Containerid
                    String[] split = amContainerLogsURL.split("/");
                    String amContainerName = split[split.length - 2];

                    List<String> collect = containerIds.stream()
                            .map(tmContainerId -> StringUtils.replace(amContainerLogsURL, amContainerName, tmContainerId))
                            .map(FunctionUtils.uncheckedFunction((containerUrl) -> applicationWSParser.parseContainerLogBaseInfo(containerUrl, logPreURL)))
                            .collect(Collectors.toList());

                    result.addAll(collect);
                }
            }
        } catch (Exception e) {
            LOG.error("getRollingLogBaseInfo error :", e);
        }

        return result;
    }

    public YarnClient buildYarnClient(org.apache.hadoop.conf.Configuration yarnConf) {
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConf);
        yarnClient.start();

        return yarnClient;
    }

    private List<String> getContainersId(String taskManagerUrl) throws IOException {
        List<String> containerIds = Lists.newArrayList();
        try {
            String taskmanagersInfo = HttpClientUtil.getRequest(taskManagerUrl);
            JSONObject response = JSONObject.parseObject(taskmanagersInfo);
            JSONArray taskmanagers = response.getJSONArray("taskmanagers");

            containerIds = IntStream.range(0, taskmanagers.size())
                    .mapToObj(taskmanagers::getJSONObject)
                    .map(jsonObject -> (String) jsonObject.get("id"))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            LOG.error("request taskmanagers error !", e);
        }
        return containerIds;
    }
}
