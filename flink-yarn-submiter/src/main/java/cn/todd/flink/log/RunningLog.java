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
import cn.todd.flink.entity.TaskmanagerInfo;
import cn.todd.flink.factory.YarnClusterClientFactory;
import cn.todd.flink.utils.ApplicationWSParser;
import cn.todd.flink.utils.HttpClientUtil;
import cn.todd.flink.utils.UrlUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
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
    private static final String CONTAINER_LOG_URL_TMP = "%s/node/containerlogs/%s/%s";
    private static final String TASK_MANAGERS_KEY = "taskmanagers";

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

                String amContainerLogsURL = applicationWSParser.getParamContent(ApplicationWSParser.AM_CONTAINER_LOGS_TAG);
                String user = applicationWSParser.getParamContent(ApplicationWSParser.AM_USER_TAG);
                String containerLogUrlFormat = UrlUtil.formatUrlHost(amContainerLogsURL);
                String trackingUrl = applicationWSParser.getParamContent(ApplicationWSParser.TRACKING_URL);

                // parse am log
                parseAmLog(applicationWSParser, amContainerLogsURL).ifPresent(result::add);
                // parse containers log
                List<String> containersLogs = parseContainersLog(applicationWSParser, user, containerLogUrlFormat, trackingUrl);
                result.addAll(containersLogs);
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

    private String buildContainerLogUrl(String containerHostPortFormat, String[] nameAndHost, String user) {
        LOG.debug("buildContainerLogUrl name:{},host{},user{} ", nameAndHost[0], nameAndHost[1], user);
        String containerUlrPre = String.format(containerHostPortFormat, nameAndHost[1]);
        return String.format(CONTAINER_LOG_URL_TMP, containerUlrPre, nameAndHost[0], user);
    }

    /**
     *  parse am log
     * @param applicationWSParser
     * @return
     */
    public Optional<String> parseAmLog(ApplicationWSParser applicationWSParser, String amContainerLogsURL) {
        try {
            String logPreURL = UrlUtil.getHttpRootURL(amContainerLogsURL);
            ApplicationWSParser.RollingBaseInfo amLogInfo = applicationWSParser.parseContainerLogBaseInfo(amContainerLogsURL, logPreURL);
            return Optional.ofNullable(JSONObject.toJSONString(amLogInfo));
        } catch (Exception e) {
            LOG.error(" parse am Log error !", e);
        }
        return Optional.empty();
    }

    private List<String> parseContainersLog(ApplicationWSParser applicationWSParser, String user, String containerLogUrlFormat, String trackingUrl) throws IOException {
        List<String> taskmanagerInfoStr = Lists.newArrayList();
        List<TaskmanagerInfo> taskmanagerInfos = getContainersNameAndHost(trackingUrl);
        for (TaskmanagerInfo info : taskmanagerInfos) {
            String[] nameAndHost = parseContainerNameAndHost(info);
            String containerLogUrl = buildContainerLogUrl(containerLogUrlFormat, nameAndHost, user);
            ApplicationWSParser.RollingBaseInfo rollingBaseInfo = applicationWSParser.parseContainerLogBaseInfo(containerLogUrl, UrlUtil.getHttpRootURL(containerLogUrl));
            rollingBaseInfo.setOtherInfo(JSONObject.toJSONString(info));
            taskmanagerInfoStr.add(JSONObject.toJSONString(rollingBaseInfo));
        }
        return taskmanagerInfoStr;
    }

    private List<TaskmanagerInfo> getContainersNameAndHost(String trackingUrl) throws IOException {
        List<TaskmanagerInfo> containersNameAndHost = Lists.newArrayList();
        try {
            String taskManagerUrl = trackingUrl + "/" + TASK_MANAGERS_KEY;
            String taskManagersInfo = HttpClientUtil.getRequest(taskManagerUrl);
            JSONObject response = JSONObject.parseObject(taskManagersInfo);
            JSONArray taskManagers = response.getJSONArray(TASK_MANAGERS_KEY);

            containersNameAndHost = IntStream.range(0, taskManagers.size())
                    .mapToObj(taskManagers::getJSONObject)
                    .map(jsonObject -> JSONObject.toJavaObject(jsonObject, TaskmanagerInfo.class))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            LOG.error("request task managers error !", e);
        }
        return containersNameAndHost;
    }

    private String[] parseContainerNameAndHost(TaskmanagerInfo taskmanagerInfo) {
        String containerName = taskmanagerInfo.getId();
        String akkaPath = taskmanagerInfo.getPath();
        String host = "";
        try {
            LOG.info("parse akkaPath: {}", akkaPath);
            host = akkaPath.split("[@:]")[2];
        } catch (Exception e) {
            LOG.error("parseContainersHost error ", e);
        }
        return new String[]{containerName, host};
    }
}
