package cn.todd.flink;

import cn.todd.flink.cli.CliFrontendParser;
import cn.todd.flink.entity.ParamsInfo;
import cn.todd.flink.entity.ResultInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Date: 2021/10/1
 *
 * @author todd5167
 */
public class CliSubmitterMain {
    private static final Logger LOG = LoggerFactory.getLogger(CliSubmitterMain.class);

    public static void main(String[] args) throws Exception {
        ParamsInfo paramsInfo = new CliFrontendParser().parseParamsInfo(args);
        LOG.info("parsed params info:{}", paramsInfo);

        ClusterClient clusterClient = ClusterClient.INSTANCE;

        ResultInfo jobResult =
                paramsInfo.isOpenSecurity()
                        ? clusterClient.submitFlinkJobWithKerberos(paramsInfo)
                        : clusterClient.submitFlinkJob(paramsInfo);

        LOG.info(
                "------------------------------------submit result--------------------------------------------");
        LOG.info(
                String.format(
                        "job submit result, appId:%s, jobId:%s, msg:%s",
                        jobResult.getAppId(), jobResult.getJobId(), jobResult.getMsg()));
        LOG.info(
                "---------------------------------------------------------------------------------------------");
    }
}
