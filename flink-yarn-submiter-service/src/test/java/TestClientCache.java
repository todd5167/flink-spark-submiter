import cn.todd.flink.client.ClientCache;
import cn.todd.flink.client.IClusterClient;
import cn.todd.flink.entity.ParamsInfo;

/**
 * Date: 2021/10/1
 *
 * @author todd5167
 */
public class TestClientCache {

    public static void main(String[] args) throws Exception {
        ParamsInfo paramsInfo =
                ParamsInfo.builder()
                        .setFlinkVersion("1.12.1")
                        .setApplicationId("application_1621864037834_0053")
                        .setHadoopConfDir("/Users/todd/flink-submitter-env/2")
                        .setFlinkConfDir("no_set")
                        .build();
        String findJarPath = "/Users/todd/flink-submitter-env/flink-client-submitter";
        IClusterClient clusterClient = ClientCache.INSTANCE.getClient(paramsInfo, findJarPath);

        int currentJobStatus = clusterClient.getYarnJobStatus(paramsInfo).getValue();
        System.out.println(currentJobStatus);
    }
}
