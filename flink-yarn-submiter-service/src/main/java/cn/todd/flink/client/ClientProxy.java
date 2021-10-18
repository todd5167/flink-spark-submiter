package cn.todd.flink.client;

import cn.todd.flink.classloader.TemporaryClassLoaderContext;
import cn.todd.flink.entity.CheckpointInfo;
import cn.todd.flink.entity.ParamsInfo;
import cn.todd.flink.entity.ResultInfo;
import cn.todd.flink.enums.ETaskStatus;

import java.util.List;

/** 通过切换classloader加载ClusterClient */
public class ClientProxy implements IClusterClient {
    IClusterClient proxyClient;

    public ClientProxy(IClusterClient proxyClient) {
        this.proxyClient = proxyClient;
    }

    @Override
    public ResultInfo submitFlinkJob(ParamsInfo jobParamsInfo) throws Exception {
        ClassLoader clientClassLoader = proxyClient.getClass().getClassLoader();
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(clientClassLoader)) {
            return proxyClient.submitFlinkJob(jobParamsInfo);
        }
    }

    @Override
    public ResultInfo submitFlinkJobWithKerberos(ParamsInfo jobParamsInfo) throws Exception {
        ClassLoader clientClassLoader = proxyClient.getClass().getClassLoader();
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(clientClassLoader)) {
            return proxyClient.submitFlinkJobWithKerberos(jobParamsInfo);
        }
    }

    @Override
    public ResultInfo killYarnJob(ParamsInfo jobParamsInfo) throws Exception {
        ClassLoader clientClassLoader = proxyClient.getClass().getClassLoader();
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(clientClassLoader)) {
            return proxyClient.killYarnJob(jobParamsInfo);
        }
    }

    @Override
    public ResultInfo killYarnJobWithKerberos(ParamsInfo jobParamsInfo) throws Exception {
        ClassLoader clientClassLoader = proxyClient.getClass().getClassLoader();
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(clientClassLoader)) {
            return proxyClient.killYarnJobWithKerberos(jobParamsInfo);
        }
    }

    @Override
    public ETaskStatus getYarnJobStatus(ParamsInfo jobParamsInfo) throws Exception {
        ClassLoader clientClassLoader = proxyClient.getClass().getClassLoader();
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(clientClassLoader)) {
            return proxyClient.getYarnJobStatus(jobParamsInfo);
        }
    }

    @Override
    public ETaskStatus getYarnJobStatusWithKerberos(ParamsInfo jobParamsInfo) throws Exception {
        ClassLoader clientClassLoader = proxyClient.getClass().getClassLoader();
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(clientClassLoader)) {
            return proxyClient.getYarnJobStatusWithKerberos(jobParamsInfo);
        }
    }

    @Override
    public List<CheckpointInfo> getCheckpointPaths(ParamsInfo jobParamsInfo) throws Exception {
        ClassLoader clientClassLoader = proxyClient.getClass().getClassLoader();
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(clientClassLoader)) {
            return proxyClient.getCheckpointPaths(jobParamsInfo);
        }
    }

    @Override
    public List<CheckpointInfo> getCheckpointPathsWithKerberos(ParamsInfo jobParamsInfo)
            throws Exception {
        ClassLoader clientClassLoader = proxyClient.getClass().getClassLoader();
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(clientClassLoader)) {
            return proxyClient.getCheckpointPathsWithKerberos(jobParamsInfo);
        }
    }

    @Override
    public String printFinishedLogToFile(ParamsInfo jobParamsInfo) throws Exception {
        ClassLoader clientClassLoader = proxyClient.getClass().getClassLoader();
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(clientClassLoader)) {
            return proxyClient.printFinishedLogToFile(jobParamsInfo);
        }
    }

    @Override
    public String printFinishedLogToFileWithKerberos(ParamsInfo jobParamsInfo) throws Exception {
        ClassLoader clientClassLoader = proxyClient.getClass().getClassLoader();
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(clientClassLoader)) {
            return proxyClient.printFinishedLogToFileWithKerberos(jobParamsInfo);
        }
    }

    @Override
    public ResultInfo cancelFlinkJob(ParamsInfo jobParamsInfo) throws Exception {
        ClassLoader clientClassLoader = proxyClient.getClass().getClassLoader();
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(clientClassLoader)) {
            return proxyClient.cancelFlinkJob(jobParamsInfo);
        }
    }

    @Override
    public ResultInfo cancelFlinkJobDoSavepoint(ParamsInfo jobParamsInfo) throws Exception {
        ClassLoader clientClassLoader = proxyClient.getClass().getClassLoader();
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(clientClassLoader)) {
            return proxyClient.cancelFlinkJobDoSavepoint(jobParamsInfo);
        }
    }

    @Override
    public ResultInfo cancelFlinkJobWithKerberos(ParamsInfo jobParamsInfo) throws Exception {
        ClassLoader clientClassLoader = proxyClient.getClass().getClassLoader();
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(clientClassLoader)) {
            return proxyClient.cancelFlinkJobWithKerberos(jobParamsInfo);
        }
    }

    @Override
    public ResultInfo cancelFlinkJobDoSavepointWithKerberos(ParamsInfo jobParamsInfo)
            throws Exception {
        ClassLoader clientClassLoader = proxyClient.getClass().getClassLoader();
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(clientClassLoader)) {
            return proxyClient.cancelFlinkJobDoSavepointWithKerberos(jobParamsInfo);
        }
    }
}
