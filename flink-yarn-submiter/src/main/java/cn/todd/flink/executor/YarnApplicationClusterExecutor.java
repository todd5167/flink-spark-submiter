package cn.todd.flink.executor;

import cn.todd.flink.entity.JobParamsInfo;
import cn.todd.flink.factory.YarnClusterClientFactory;
import cn.todd.flink.utils.JobGraphBuildUtil;
import org.apache.commons.math3.util.Pair;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import java.util.Optional;

public class YarnApplicationClusterExecutor extends AbstractClusterExecutor {
    public YarnApplicationClusterExecutor(JobParamsInfo jobParamsInfo) {
        super(jobParamsInfo);
    }

    @Override
    public Optional<Pair<String, String>> submit() throws Exception {
        Configuration flinkConfiguration = JobGraphBuildUtil.getFlinkConfiguration(jobParamsInfo.getFlinkConfDir());
        ClusterSpecification clusterSpecification = YarnClusterClientFactory.INSTANCE.getClusterSpecification(flinkConfiguration);
        ApplicationConfiguration applicationConfiguration =
                new ApplicationConfiguration(
                        jobParamsInfo.getExecArgs(), jobParamsInfo.getEntryPointClassName());

        try (YarnClusterDescriptor clusterDescriptor = (YarnClusterDescriptor) YarnClusterClientFactory.INSTANCE
                .createClusterDescriptor(jobParamsInfo.getYarnConfDir(), flinkConfiguration)) {

            ClusterClientProvider<ApplicationId> application = clusterDescriptor.deployApplicationCluster(clusterSpecification, applicationConfiguration);
            String applicationId = application.getClusterClient().getClusterId().toString();
            return Optional.of(new Pair<>(applicationId, null));
        }
    }
}
