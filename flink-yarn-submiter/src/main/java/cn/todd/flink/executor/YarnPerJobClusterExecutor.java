package cn.todd.flink.executor;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.yarn.YarnClusterDescriptor;

import cn.todd.flink.entity.ParamsInfo;
import cn.todd.flink.entity.ResultInfo;
import cn.todd.flink.factory.YarnClusterDescriptorFactory;
import cn.todd.flink.utils.JobGraphBuildUtil;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.util.List;

/**
 * Date: 2021/10/1
 *
 * @author todd5167
 */
public class YarnPerJobClusterExecutor extends AbstractClusterExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(YarnPerJobClusterExecutor.class);

    public YarnPerJobClusterExecutor(ParamsInfo jobParamsInfo) {
        super(jobParamsInfo);
    }

    @Override
    public ResultInfo submitJob() {
        try {
            // 1. parse default flink configuration from flink-conf.yaml and dynamic replacement
            // default config.
            Configuration flinkConfig = getFlinkConfigFromParamsInfo();

            // 2. build JobGraph from user program.
            JobGraph jobGraph = JobGraphBuildUtil.buildJobGraph(jobParamsInfo, flinkConfig);
            LOG.info("build job graph success!");

            // 3. build the submitted yarn environment.
            try (YarnClusterDescriptor clusterDescriptor =
                    (YarnClusterDescriptor)
                            YarnClusterDescriptorFactory.INSTANCE.createClusterDescriptor(
                                    jobParamsInfo.getHadoopConfDir(), flinkConfig)) {

                // 4. replace flinkJarPath and ship flink lib jars.
                replaceFlinkJarPathAndShipLibJars(
                        jobParamsInfo.getFlinkJarPath(), clusterDescriptor);

                // 5. deploy JobGraph to yarn.
                ClusterSpecification clusterSpecification =
                        YarnClusterDescriptorFactory.INSTANCE.getClusterSpecification(flinkConfig);
                ClusterClientProvider<ApplicationId> applicationIdClusterClientProvider =
                        clusterDescriptor.deployJobCluster(clusterSpecification, jobGraph, true);

                String applicationId =
                        applicationIdClusterClientProvider
                                .getClusterClient()
                                .getClusterId()
                                .toString();
                String jobId = jobGraph.getJobID().toString();
                LOG.info("deploy per_job with appId: {}, jobId: {}", applicationId, jobId);

                return new ResultInfo(applicationId, jobId, "");
            }
        } catch (Exception e) {
            LOG.error("submit job to yarn error:{}", e);
            return new ResultInfo("", "", ExceptionUtils.getStackTrace(e));
        }
    }

    /**
     * 1. flink jar path use flinkJarPath/flink-dist.jar.
     *
     * <p>2. upload flinkJarPath jars.
     *
     * @param flinkJarPath
     * @param clusterDescriptor
     * @return
     * @throws MalformedURLException
     */
    private List<File> replaceFlinkJarPathAndShipLibJars(
            String flinkJarPath, YarnClusterDescriptor clusterDescriptor)
            throws MalformedURLException {
        if (StringUtils.isEmpty(flinkJarPath) || !new File(flinkJarPath).exists()) {
            throw new RuntimeException("The param '-flinkJarPath' ref dir is not exist");
        }
        File[] jars = new File(flinkJarPath).listFiles();
        List<File> shipFiles = Lists.newArrayList();
        for (File file : jars) {
            if (file.toURI().toURL().toString().contains("flink-dist")) {
                clusterDescriptor.setLocalJarPath(new Path(file.toURI().toURL().toString()));
            } else {
                shipFiles.add(file);
            }
        }

        clusterDescriptor.addShipFiles(shipFiles);
        return shipFiles;
    }
}
