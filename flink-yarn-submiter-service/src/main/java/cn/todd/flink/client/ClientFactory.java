package cn.todd.flink.client;

import cn.todd.flink.classloader.ChildFirstClassLoader;
import cn.todd.flink.classloader.TemporaryClassLoaderContext;
import cn.todd.flink.entity.ParamsInfo;
import cn.todd.flink.entity.ResultInfo;
import cn.todd.flink.enums.ETaskStatus;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

public class ClientFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ClientFactory.class);

    private static Map<String, ClassLoader> clientClassLoader = Maps.newConcurrentMap();

    private static String[] alwaysParentFirstPatterns =
            new String[] {
                IClusterClient.class.getName(),
                ResultInfo.class.getName(),
                ParamsInfo.class.getName(),
                ETaskStatus.class.getName()
            };

    public static IClusterClient createClient(String clientType, String findJarPath)
            throws Exception {
        ClassLoader classLoader =
                clientClassLoader.computeIfAbsent(
                        clientType,
                        type -> {
                            String findClientPath = String.format("%s/%s/lib", findJarPath, type);
                            LOG.info(
                                    "current type is :{}, find client path is : {} ",
                                    clientType,
                                    findClientPath);

                            File clientFile = new File(findClientPath);
                            if (!clientFile.exists() || clientFile.list().length == 0) {
                                throw new RuntimeException(
                                        String.format(
                                                "%s directory not found or directory no file",
                                                findClientPath));
                            }
                            return createDispatcherClassLoader(clientFile);
                        });

        try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classLoader)) {
            ServiceLoader<IClusterClient> serviceLoader = ServiceLoader.load(IClusterClient.class);
            // create client
            List<IClusterClient> matchingClient = new ArrayList<>();
            serviceLoader.iterator().forEachRemaining(matchingClient::add);

            if (matchingClient.size() != 1) {
                throw new RuntimeException(
                        "zero or more than one plugin client found" + matchingClient);
            }
            return new ClientProxy(matchingClient.get(0));
        }
    }

    /**
     * 根据传递的client插件包路径，创建自定义类加载器
     *
     * @param dir
     * @return
     */
    private static URLClassLoader createDispatcherClassLoader(File dir) {
        File[] files = dir.listFiles();
        URL[] urls =
                Arrays.stream(files)
                        .filter(file -> file.isFile() && file.getName().endsWith(".jar"))
                        .map(
                                file -> {
                                    try {
                                        return file.toURI().toURL();
                                    } catch (MalformedURLException e) {
                                        throw new RuntimeException("file to url error ", e);
                                    }
                                })
                        .toArray(URL[]::new);
        return new ChildFirstClassLoader(
                urls, ClientFactory.class.getClassLoader(), alwaysParentFirstPatterns);
    }
}
