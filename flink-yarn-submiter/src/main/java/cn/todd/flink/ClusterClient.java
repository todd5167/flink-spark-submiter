package cn.todd.flink;

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.FunctionUtils;

import cn.todd.flink.client.IClusterClient;
import cn.todd.flink.entity.CheckpointInfo;
import cn.todd.flink.entity.ParamsInfo;
import cn.todd.flink.entity.ResultInfo;
import cn.todd.flink.enums.ERunMode;
import cn.todd.flink.enums.ETaskStatus;
import cn.todd.flink.executor.KerberosSecurityContext;
import cn.todd.flink.executor.YarnApplicationClusterExecutor;
import cn.todd.flink.executor.YarnPerJobClusterExecutor;
import cn.todd.flink.factory.YarnClusterDescriptorFactory;
import cn.todd.flink.utils.HdfsUtil;
import cn.todd.flink.utils.YarnLogHelper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Date: 2021/10/1
 *
 * @author todd5167
 */
public enum ClusterClient implements IClusterClient {
    INSTANCE;

    private static final Logger LOG = LoggerFactory.getLogger(ClusterClient.class);

    private static Cache<String, YarnClient> YARN_CLIENT_CACHE =
            CacheBuilder.newBuilder()
                    .expireAfterWrite(12, TimeUnit.HOURS)
                    .removalListener(new YarnClientRemovalListener())
                    .build();

    @Override
    public ResultInfo submitFlinkJob(ParamsInfo jobParamsInfo) throws Exception {
        String mode =
                StringUtils.isEmpty(jobParamsInfo.getRunMode())
                        ? "YARN_PERJOB"
                        : jobParamsInfo.getRunMode();
        ERunMode runMode = ERunMode.convertFromString(mode);
        ResultInfo resultInfo;
        switch (runMode) {
            case YARN_APPLICATION:
                resultInfo = new YarnApplicationClusterExecutor(jobParamsInfo).submitJob();
                break;
            case YARN_PERJOB:
            default:
                resultInfo = new YarnPerJobClusterExecutor(jobParamsInfo).submitJob();
        }
        return resultInfo;
    }

    @Override
    public ResultInfo submitFlinkJobWithKerberos(ParamsInfo jobParamsInfo) throws Exception {
        return KerberosSecurityContext.runSecured(
                jobParamsInfo,
                FunctionUtils.uncheckedSupplier(() -> submitFlinkJob(jobParamsInfo)));
    }

    @Override
    public ResultInfo killYarnJob(ParamsInfo jobParamsInfo) throws IOException {
        return new YarnPerJobClusterExecutor(jobParamsInfo).killJob();
    }

    @Override
    public ResultInfo killYarnJobWithKerberos(ParamsInfo jobParamsInfo) throws Exception {
        return KerberosSecurityContext.runSecured(
                jobParamsInfo, FunctionUtils.uncheckedSupplier(() -> killYarnJob(jobParamsInfo)));
    }

    @Override
    public ETaskStatus getYarnJobStatus(ParamsInfo jobParamsInfo) throws Exception {
        String applicationId = jobParamsInfo.getApplicationId();
        String hadoopConfDir = jobParamsInfo.getHadoopConfDir();
        Preconditions.checkNotNull(applicationId, "yarn applicationId is not null!");
        Preconditions.checkNotNull(hadoopConfDir, "hadoop conf dir is not null!");

        YarnClient yarnClient =
                YARN_CLIENT_CACHE.get(
                        hadoopConfDir,
                        () -> {
                            try {
                                LOG.info("create yarn client,create time:{}", LocalDateTime.now());
                                return new YarnPerJobClusterExecutor(jobParamsInfo)
                                        .createYarnClient();
                            } catch (IOException e) {
                                LOG.error("create yarn client error!", e);
                            }
                            return null;
                        });

        if (!Objects.isNull(yarnClient)) {
            try {
                ApplicationId appId = ConverterUtils.toApplicationId(applicationId);
                ApplicationReport report = yarnClient.getApplicationReport(appId);

                YarnApplicationState yarnApplicationState = report.getYarnApplicationState();
                switch (yarnApplicationState) {
                    case NEW:
                    case NEW_SAVING:
                    case SUBMITTED:
                    case ACCEPTED:
                        return ETaskStatus.ACCEPTED;
                    case RUNNING:
                        return ETaskStatus.RUNNING;
                    case FINISHED:
                        FinalApplicationStatus finalApplicationStatus =
                                report.getFinalApplicationStatus();
                        if (finalApplicationStatus == FinalApplicationStatus.FAILED) {
                            return ETaskStatus.FAILED;
                        } else if (finalApplicationStatus == FinalApplicationStatus.KILLED) {
                            return ETaskStatus.KILLED;
                        } else {
                            // UNDEFINED define SUCCEEDED
                            return ETaskStatus.SUCCEEDED;
                        }
                    case FAILED:
                        return ETaskStatus.FAILED;
                    case KILLED:
                        return ETaskStatus.KILLED;
                    default:
                        throw new RuntimeException("Unsupported application state");
                }
            } catch (Exception e) {
                LOG.error("get yarn job status error!", e);
                return ETaskStatus.NOT_FOUND;
            }
        }
        return ETaskStatus.NOT_FOUND;
    }

    @Override
    public ETaskStatus getYarnJobStatusWithKerberos(ParamsInfo jobParamsInfo) throws Exception {
        return KerberosSecurityContext.runSecured(
                jobParamsInfo,
                FunctionUtils.uncheckedSupplier(() -> getYarnJobStatus(jobParamsInfo)));
    }

    @Override
    public List<CheckpointInfo> getCheckpointPaths(ParamsInfo jobParamsInfo) throws Exception {
        YarnConfiguration yarnConfiguration =
                YarnClusterDescriptorFactory.INSTANCE.parseYarnConfFromConfDir(
                        jobParamsInfo.getHadoopConfDir());
        FileSystem fileSystem = FileSystem.get(yarnConfiguration);

        List<CheckpointInfo> checkpointInfos =
                HdfsUtil.listFiles(
                        fileSystem,
                        new Path(jobParamsInfo.getHdfsPath()),
                        (Path file) -> file.getName().startsWith("chk-"));
        return checkpointInfos;
    }

    @Override
    public List<CheckpointInfo> getCheckpointPathsWithKerberos(ParamsInfo jobParamsInfo)
            throws Exception {
        return KerberosSecurityContext.runSecured(
                jobParamsInfo,
                FunctionUtils.uncheckedSupplier(() -> getCheckpointPaths(jobParamsInfo)));
    }

    @Override
    public String printFinishedLogToFile(ParamsInfo jobParamsInfo) throws Exception {
        String applicationId = jobParamsInfo.getApplicationId();
        String finishedJobLogDir = jobParamsInfo.getFinishedJobLogDir();

        YarnConfiguration yarnConfiguration =
                YarnClusterDescriptorFactory.INSTANCE.parseYarnConfFromConfDir(
                        jobParamsInfo.getHadoopConfDir());

        return YarnLogHelper.printAllContainersLogsReturnFilePath(
                yarnConfiguration, finishedJobLogDir, applicationId);
    }

    @Override
    public String printFinishedLogToFileWithKerberos(ParamsInfo jobParamsInfo) throws Exception {
        return KerberosSecurityContext.runSecured(
                jobParamsInfo,
                FunctionUtils.uncheckedSupplier(() -> printFinishedLogToFile(jobParamsInfo)));
    }

    @Override
    public ResultInfo cancelFlinkJob(ParamsInfo jobParamsInfo) throws Exception {
        Preconditions.checkNotNull(
                jobParamsInfo.getHadoopConfDir(), "cancel job hadoopConfDir is required!");
        Preconditions.checkNotNull(
                jobParamsInfo.getApplicationId(), "cancel job applicationId is required!");
        Preconditions.checkNotNull(
                jobParamsInfo.getFlinkJobId(), "cancel job flinkJobId is required!");

        return new YarnPerJobClusterExecutor(jobParamsInfo).cancelJob(false);
    }

    @Override
    public ResultInfo cancelFlinkJobWithKerberos(ParamsInfo jobParamsInfo) throws Exception {
        return KerberosSecurityContext.runSecured(
                jobParamsInfo,
                FunctionUtils.uncheckedSupplier(() -> cancelFlinkJob(jobParamsInfo)));
    }

    @Override
    public ResultInfo cancelFlinkJobDoSavepoint(ParamsInfo jobParamsInfo) throws Exception {
        Preconditions.checkNotNull(
                jobParamsInfo.getHadoopConfDir(), "cancel job hadoopConfDir is required!");
        Preconditions.checkNotNull(
                jobParamsInfo.getApplicationId(), "cancel job applicationId is required!");
        Preconditions.checkNotNull(
                jobParamsInfo.getFlinkJobId(), "cancel job flinkJobId is required!");

        return new YarnPerJobClusterExecutor(jobParamsInfo).cancelJob(true);
    }

    @Override
    public ResultInfo cancelFlinkJobDoSavepointWithKerberos(ParamsInfo jobParamsInfo)
            throws Exception {
        return KerberosSecurityContext.runSecured(
                jobParamsInfo,
                FunctionUtils.uncheckedSupplier(() -> cancelFlinkJobDoSavepoint(jobParamsInfo)));
    }

    private static class YarnClientRemovalListener implements RemovalListener<String, YarnClient> {
        @Override
        public void onRemoval(RemovalNotification<String, YarnClient> clientCache) {
            LOG.info(
                    "remove cache key={},value={},reason={},time:{}",
                    clientCache.getKey(),
                    clientCache.getValue(),
                    clientCache.getCause(),
                    LocalDateTime.now());
            Optional.ofNullable(clientCache.getValue()).ifPresent(YarnClient::stop);
        }
    }
}
