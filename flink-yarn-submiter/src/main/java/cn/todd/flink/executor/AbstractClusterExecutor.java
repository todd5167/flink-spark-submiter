package cn.todd.flink.executor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.FlinkException;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnConfigOptionsInternal;

import cn.todd.flink.entity.ParamsInfo;
import cn.todd.flink.entity.ResultInfo;
import cn.todd.flink.factory.YarnClusterDescriptorFactory;
import cn.todd.flink.utils.JobGraphBuildUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.yarn.configuration.YarnLogConfigUtil.CONFIG_FILE_LOG4J_NAME;
import static org.apache.flink.yarn.configuration.YarnLogConfigUtil.CONFIG_FILE_LOGBACK_NAME;

/**
 * Date: 2021/10/1
 *
 * @author todd5167
 */
public abstract class AbstractClusterExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractClusterExecutor.class);

    private static final String DEFAULT_TOTAL_PROCESS_MEMORY = "1024m";

    public ParamsInfo jobParamsInfo;

    public AbstractClusterExecutor(ParamsInfo jobParamsInfo) {
        this.jobParamsInfo = jobParamsInfo;
    }

    /**
     * submit job
     *
     * @return
     */
    public abstract ResultInfo submitJob();

    public YarnClient createYarnClient() throws IOException {
        YarnClient yarnClient =
                YarnClusterDescriptorFactory.INSTANCE.createYarnClientFromHadoopConfDir(
                        jobParamsInfo.getHadoopConfDir());
        LOG.info(
                "yarn client successfully created, hadoop conf dir:{}",
                jobParamsInfo.getHadoopConfDir());
        return yarnClient;
    }

    /**
     * kill yarn job and clean application files in hdfs.
     *
     * @return
     */
    public ResultInfo killJob() throws IOException {
        String applicationId = jobParamsInfo.getApplicationId();
        if (StringUtils.isEmpty(applicationId)) {
            throw new NullPointerException("kill yarn job applicationId is required!");
        }
        LOG.info("killed applicationId is:{}", applicationId);
        YarnConfiguration yarnConfiguration =
                YarnClusterDescriptorFactory.INSTANCE.parseYarnConfFromConfDir(
                        jobParamsInfo.getHadoopConfDir());

        try (YarnClient yarnClient =
                YarnClusterDescriptorFactory.INSTANCE.createYarnClientFromYarnConf(
                        yarnConfiguration); ) {
            yarnClient.killApplication(ConverterUtils.toApplicationId(applicationId));
            LOG.info("killed applicationId {} was unsuccessful.", applicationId);
        } catch (YarnException e) {
            LOG.error("killed applicationId {} was failed.", e);
            return new ResultInfo("", "", ExceptionUtils.getStackTrace(e));
        }

        try (FileSystem fs = FileSystem.get(yarnConfiguration)) {
            Path applicationDir =
                    new Path(
                            checkNotNull(fs.getHomeDirectory()),
                            ".flink/" + checkNotNull(applicationId) + '/');
            if (!fs.delete(applicationDir, true)) {
                LOG.error(
                        "Deleting yarn application files under {} was unsuccessful.",
                        applicationDir);
            } else {
                LOG.error(
                        "Deleting yarn application files under {} was successful.", applicationDir);
            }
        } catch (Exception e) {
            LOG.error("Deleting yarn application files was failed!", e);
        }
        return new ResultInfo("", "", "");
    }

    public ResultInfo cancelJob(boolean doSavepoint) {
        String appId = jobParamsInfo.getApplicationId();
        String jobId = jobParamsInfo.getFlinkJobId();

        LOG.info("cancel Job appId:{}, jobId:{}", appId, jobId);

        ApplicationId applicationId = ConverterUtils.toApplicationId(appId);
        JobID flinkJobId = new JobID(org.apache.flink.util.StringUtils.hexStringToByte(jobId));

        Configuration flinkConfig = getFlinkConfigFromParamsInfo();
        try (YarnClusterDescriptor clusterDescriptor =
                (YarnClusterDescriptor)
                        YarnClusterDescriptorFactory.INSTANCE.createClusterDescriptor(
                                jobParamsInfo.getHadoopConfDir(), flinkConfig)) {

            ClusterClientProvider<ApplicationId> retrieve =
                    clusterDescriptor.retrieve(applicationId);
            try (ClusterClient<ApplicationId> clusterClient = retrieve.getClusterClient()) {
                if (doSavepoint) {
                    CompletableFuture<String> savepointFuture =
                            clusterClient.cancelWithSavepoint(flinkJobId, null);
                    Object result = savepointFuture.get(2, TimeUnit.MINUTES);
                    LOG.info("flink job savepoint path: {}", result.toString());
                } else {
                    CompletableFuture<Acknowledge> cancelFuture = clusterClient.cancel(flinkJobId);
                    Object result = cancelFuture.get(2, TimeUnit.MINUTES);
                    LOG.info("flink job cancel result: {}", result.toString());
                }
            } catch (Exception e) {
                try {
                    LOG.error("cancel job error, will kill job:", e);
                    clusterDescriptor.killCluster(applicationId);
                } catch (FlinkException e1) {
                    LOG.error("yarn cluster Descriptor kill cluster error:", e);
                    return new ResultInfo("", "", "error");
                }
            }

        } catch (Exception e) {
            LOG.error(String.format("cancel job failed,appId:{}, jobId:", appId, jobId), e);
            return new ResultInfo("", "", "error");
        }

        return new ResultInfo("", "", "success");
    }

    protected Configuration getFlinkConfigFromParamsInfo() {
        Configuration defaultGlobalConfig =
                JobGraphBuildUtil.getFlinkConfiguration(jobParamsInfo.getFlinkConfDir());
        replaceDefaultGlobalConfig(defaultGlobalConfig, jobParamsInfo);
        return defaultGlobalConfig;
    }

    /**
     * replace the default configuration items in the flink-conf.yaml
     *
     * @param flinkConfig
     * @param jobParamsInfo
     */
    protected void replaceDefaultGlobalConfig(Configuration flinkConfig, ParamsInfo jobParamsInfo) {
        if (!StringUtils.isEmpty(jobParamsInfo.getName())) {
            flinkConfig.setString(YarnConfigOptions.APPLICATION_NAME, jobParamsInfo.getName());
        }

        if (!StringUtils.isEmpty(jobParamsInfo.getQueue())) {
            flinkConfig.setString(YarnConfigOptions.APPLICATION_QUEUE, jobParamsInfo.getQueue());
        }

        if (!StringUtils.isEmpty(jobParamsInfo.getFlinkConfDir())) {
            discoverLogConfigFile(jobParamsInfo.getFlinkConfDir())
                    .ifPresent(
                            file ->
                                    flinkConfig.setString(
                                            YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE,
                                            file.getPath()));
        }

        if (!flinkConfig.contains(TaskManagerOptions.TOTAL_PROCESS_MEMORY)) {
            flinkConfig.setString(
                    TaskManagerOptions.TOTAL_PROCESS_MEMORY.key(), DEFAULT_TOTAL_PROCESS_MEMORY);
        }

        // fill security config
        if (jobParamsInfo.isOpenSecurity()) {
            flinkConfig.setString(
                    SecurityOptions.KERBEROS_LOGIN_KEYTAB.key(), jobParamsInfo.getKeytabPath());
            Optional.ofNullable(jobParamsInfo.getPrincipal())
                    .ifPresent(
                            principal ->
                                    flinkConfig.setString(
                                            SecurityOptions.KERBEROS_LOGIN_PRINCIPAL.key(),
                                            principal));
        }

        Properties flinkConfigProperties = jobParamsInfo.getConfProperties();
        if (!Objects.isNull(flinkConfigProperties)) {
            flinkConfigProperties.stringPropertyNames().stream()
                    .forEach(
                            key ->
                                    flinkConfig.setString(
                                            key, flinkConfigProperties.getProperty(key)));
        }
    }

    /**
     * find log4 files from flink conf
     *
     * @param configurationDirectory
     * @return
     */
    protected Optional<File> discoverLogConfigFile(final String configurationDirectory) {
        Optional<File> logConfigFile = Optional.empty();

        final File log4jFile =
                new File(configurationDirectory + File.separator + CONFIG_FILE_LOG4J_NAME);
        if (log4jFile.exists()) {
            logConfigFile = Optional.of(log4jFile);
        }

        final File logbackFile =
                new File(configurationDirectory + File.separator + CONFIG_FILE_LOGBACK_NAME);
        if (logbackFile.exists()) {
            if (logConfigFile.isPresent()) {
                LOG.warn(
                        "The configuration directory ('"
                                + configurationDirectory
                                + "') already contains a LOG4J config file."
                                + "If you want to use logback, then please delete or rename the log configuration file.");
            } else {
                logConfigFile = Optional.of(logbackFile);
            }
        }
        return logConfigFile;
    }
}
