package cn.todd.flink.executor;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;

import cn.todd.flink.entity.ParamsInfo;
import cn.todd.flink.entity.ResultInfo;
import cn.todd.flink.factory.YarnClusterDescriptorFactory;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Date: 2021/10/1
 *
 * @author todd5167
 */
public class YarnApplicationClusterExecutor extends AbstractClusterExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(YarnApplicationClusterExecutor.class);

    public YarnApplicationClusterExecutor(ParamsInfo jobParamsInfo) {
        super(jobParamsInfo);
    }

    @Override
    public ResultInfo submitJob() {
        try {
            Configuration flinkConfig = getFlinkConfigFromParamsInfo();
            flinkConfig.setString(
                    DeploymentOptions.TARGET, YarnDeploymentTarget.APPLICATION.getName());
            ClusterSpecification clusterSpecification =
                    YarnClusterDescriptorFactory.INSTANCE.getClusterSpecification(flinkConfig);

            ApplicationConfiguration applicationConfiguration =
                    new ApplicationConfiguration(
                            jobParamsInfo.getExecArgs(), jobParamsInfo.getEntryPointClassName());
            try (YarnClusterDescriptor clusterDescriptor =
                    (YarnClusterDescriptor)
                            YarnClusterDescriptorFactory.INSTANCE.createClusterDescriptor(
                                    jobParamsInfo.getHadoopConfDir(), flinkConfig)) {
                ClusterClientProvider<ApplicationId> application =
                        clusterDescriptor.deployApplicationCluster(
                                clusterSpecification, applicationConfiguration);
                String applicationId = application.getClusterClient().getClusterId().toString();
                return new ResultInfo(applicationId, "", "submit job to yarn success!");
            }
        } catch (Exception e) {
            LOG.error("submit job to yarn error:{}", e);
            return new ResultInfo("", "", ExceptionUtils.getStackTrace(e));
        }
    }
}
