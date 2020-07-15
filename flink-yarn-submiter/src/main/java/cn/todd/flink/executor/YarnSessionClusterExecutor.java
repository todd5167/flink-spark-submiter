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

package cn.todd.flink.executor;


import cn.todd.flink.entity.JobParamsInfo;
import cn.todd.flink.enums.ETaskStatus;
import cn.todd.flink.factory.YarnClusterClientFactory;
import cn.todd.flink.utils.HttpClientUtil;
import cn.todd.flink.utils.JobGraphBuildUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterRetrieveException;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;


/**
 *
 * Date: 2020/6/14
 * @author todd5167
 */
public class YarnSessionClusterExecutor extends AbstractClusterExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(YarnSessionClusterExecutor.class);

    public YarnSessionClusterExecutor(JobParamsInfo jobParamsInfo) {
        super(jobParamsInfo);
    }

    @Override
    public Optional<Pair<String, String>> submit() throws Exception {
        JobGraph jobGraph = JobGraphBuildUtil.buildJobGraph(jobParamsInfo);
        Optional.ofNullable(jobParamsInfo.getDependFile())
                .ifPresent(files -> JobGraphBuildUtil.fillDependFilesJobGraph(jobGraph, files));

        ClusterClient clusterClient = retrieveClusterClient();
        JobExecutionResult jobExecutionResult = ClientUtils.submitJob(clusterClient, jobGraph);
        LOG.info("jobID:{}", jobExecutionResult.getJobID().toString());

        Object yid = jobParamsInfo.getYarnSessionConfProperties().get("yid");
        return Optional.of(new Pair<>(yid.toString(), jobExecutionResult.getJobID().toString()));
    }

    @Override
    public ClusterClient retrieveClusterClient() throws ClusterRetrieveException {
        Object yid = jobParamsInfo.getYarnSessionConfProperties().get("yid");
        Preconditions.checkNotNull(yid, "yarnsession mode applicationId required!");

        ApplicationId applicationId = ConverterUtils.toApplicationId(yid.toString());
        Configuration flinkConfiguration = JobGraphBuildUtil.getFlinkConfiguration(jobParamsInfo.getFlinkConfDir());
        ClusterDescriptor clusterDescriptor = YarnClusterClientFactory.INSTANCE.createClusterDescriptor(jobParamsInfo.getYarnConfDir(), flinkConfiguration);

        ClusterClientProvider<ApplicationId> retrieve = clusterDescriptor.retrieve(applicationId);
        ClusterClient<ApplicationId> clusterClient = retrieve.getClusterClient();
        return clusterClient;
    }
}
