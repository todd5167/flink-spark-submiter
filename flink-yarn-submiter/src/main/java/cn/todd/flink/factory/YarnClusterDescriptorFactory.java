package cn.todd.flink.factory;

import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.function.FunctionUtils;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.io.IOException;

/**
 * Date: 2021/10/1
 *
 * @author todd5167
 */
public enum YarnClusterDescriptorFactory implements AbstractClusterDescriptorFactory {
    INSTANCE;

    private static final String XML_FILE_EXTENSION = "xml";

    @Override
    public ClusterDescriptor createClusterDescriptor(
            String hadoopConfDir, Configuration flinkConfig) {
        if (StringUtils.isNotBlank(hadoopConfDir)) {
            try {
                YarnConfiguration yarnConf = parseYarnConfFromConfDir(hadoopConfDir);
                YarnClient yarnClient = createYarnClientFromYarnConf(yarnConf);

                YarnClusterDescriptor clusterDescriptor =
                        new YarnClusterDescriptor(
                                flinkConfig,
                                yarnConf,
                                yarnClient,
                                YarnClientYarnClusterInformationRetriever.create(yarnClient),
                                false);
                return clusterDescriptor;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new RuntimeException("yarn mode must set param of 'hadoopConfDir'");
        }
    }

    public YarnClient createYarnClientFromYarnConf(YarnConfiguration yarnConf) {
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConf);
        yarnClient.start();
        return yarnClient;
    }

    public YarnClient createYarnClientFromHadoopConfDir(String hadoopConf) throws IOException {
        YarnConfiguration yarnConf = parseYarnConfFromConfDir(hadoopConf);
        YarnClient yarnClient = createYarnClientFromYarnConf(yarnConf);
        return yarnClient;
    }

    public YarnConfiguration parseYarnConfFromConfDir(String hadoopConfDir) throws IOException {
        YarnConfiguration yarnConf = new YarnConfiguration();
        FileUtils.listFilesInDirectory(new File(hadoopConfDir).toPath(), this::isXmlFile).stream()
                .map(FunctionUtils.uncheckedFunction(FileUtils::toURL))
                .forEach(yarnConf::addResource);
        return yarnConf;
    }

    private boolean isXmlFile(java.nio.file.Path file) {
        return XML_FILE_EXTENSION.equals(
                org.apache.flink.shaded.guava18.com.google.common.io.Files.getFileExtension(
                        file.toString()));
    }
}
