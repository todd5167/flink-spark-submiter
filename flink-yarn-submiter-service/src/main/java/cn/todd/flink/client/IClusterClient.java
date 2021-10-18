package cn.todd.flink.client;

import cn.todd.flink.entity.CheckpointInfo;
import cn.todd.flink.entity.ParamsInfo;
import cn.todd.flink.entity.ResultInfo;
import cn.todd.flink.enums.ETaskStatus;

import java.util.List;

/**
 * Date: 2021/10/1
 *
 * @author todd5167
 */
public interface IClusterClient {
    /**
     * @param jobParamsInfo
     * @return
     * @throws Exception
     */
    ResultInfo submitFlinkJob(ParamsInfo jobParamsInfo) throws Exception;

    /**
     * submit flink job in kerberos env
     *
     * @param jobParamsInfo
     * @return
     * @throws Exception
     */
    ResultInfo submitFlinkJobWithKerberos(ParamsInfo jobParamsInfo) throws Exception;

    /**
     * kill flink job in yarn and delete application files in hdfs.
     *
     * @param jobParamsInfo
     * @return
     * @throws Exception
     */
    ResultInfo killYarnJob(ParamsInfo jobParamsInfo) throws Exception;

    /**
     * kill yarn job in kerberos env
     *
     * @param jobParamsInfo
     * @return
     * @throws Exception
     */
    ResultInfo killYarnJobWithKerberos(ParamsInfo jobParamsInfo) throws Exception;

    /**
     * get yarn job status
     *
     * @param jobParamsInfo
     * @return
     * @throws Exception
     */
    ETaskStatus getYarnJobStatus(ParamsInfo jobParamsInfo) throws Exception;

    /**
     * get yarn job status
     *
     * @param jobParamsInfo
     * @return
     * @throws Exception
     */
    ETaskStatus getYarnJobStatusWithKerberos(ParamsInfo jobParamsInfo) throws Exception;

    /**
     * get checkpoint path
     *
     * @param jobParamsInfo
     * @return
     * @throws Exception
     */
    List<CheckpointInfo> getCheckpointPaths(ParamsInfo jobParamsInfo) throws Exception;

    /**
     * get checkpoint path
     *
     * @param jobParamsInfo
     * @return
     * @throws Exception
     */
    List<CheckpointInfo> getCheckpointPathsWithKerberos(ParamsInfo jobParamsInfo) throws Exception;

    /**
     * print finished job logs
     *
     * @param jobParamsInfo
     * @return file path
     * @throws Exception
     */
    String printFinishedLogToFile(ParamsInfo jobParamsInfo) throws Exception;

    /**
     * print finished job logs
     *
     * @param jobParamsInfo
     * @return file path
     * @throws Exception
     */
    String printFinishedLogToFileWithKerberos(ParamsInfo jobParamsInfo) throws Exception;

    /**
     * cancel flink job by applicationId and flink job id
     *
     * @param jobParamsInfo
     * @return file path
     * @throws Exception
     */
    ResultInfo cancelFlinkJob(ParamsInfo jobParamsInfo) throws Exception;

    /**
     * cancel flink job by applicationId and flink job id
     *
     * @param jobParamsInfo
     * @return file path
     * @throws Exception
     */
    ResultInfo cancelFlinkJobDoSavepoint(ParamsInfo jobParamsInfo) throws Exception;

    /**
     * cancel flink job by applicationId and flink job id with Kerberos
     *
     * @param jobParamsInfo
     * @return file path
     * @throws Exception
     */
    ResultInfo cancelFlinkJobWithKerberos(ParamsInfo jobParamsInfo) throws Exception;

    /**
     * cancel flink job by applicationId and flink job id with Kerberos
     *
     * @param jobParamsInfo
     * @return file path
     * @throws Exception
     */
    ResultInfo cancelFlinkJobDoSavepointWithKerberos(ParamsInfo jobParamsInfo) throws Exception;
}
