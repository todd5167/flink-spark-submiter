package cn.todd.flink.client;

import cn.todd.flink.entity.ParamsInfo;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLEncoder;
import java.util.Map;

/** 缓存ClusterClient, 1.防止内存泄露.2.复用ClusterClient. */
public enum ClientCache {
    INSTANCE;

    private static final Logger LOG = LoggerFactory.getLogger(ClientCache.class);

    private Map<String, Map<String, IClusterClient>> clientCache = Maps.newConcurrentMap();

    private Map<String, Object> parallelLockMap = Maps.newConcurrentMap();

    public IClusterClient getClient(ParamsInfo jobInfo, String findJarPath) {
        try {
            String clientType = jobInfo.getFlinkVersion();
            String cacheKeyContent =
                    clientType + jobInfo.getHadoopConfDir() + jobInfo.getFlinkConfDir();
            String cacheKey = URLEncoder.encode(cacheKeyContent, "UTF-8");

            Map<String, IClusterClient> clientMap =
                    clientCache.computeIfAbsent(clientType, k -> Maps.newConcurrentMap());
            IClusterClient client = clientMap.get(cacheKey);

            if (client == null) {
                synchronized (getClientLoadingLock(cacheKey)) {
                    client = clientMap.get(cacheKey);
                    if (client == null) {
                        client = ClientFactory.createClient(jobInfo.getFlinkVersion(), findJarPath);
                        clientMap.putIfAbsent(cacheKey, client);
                    }
                }
            }
            return client;
        } catch (Throwable e) {
            LOG.error("get client error!", e);
            return null;
        }
    }

    private Object getClientLoadingLock(String clientKey) {
        Object lock = this;
        if (parallelLockMap != null) {
            Object newLock = new Object();
            lock = parallelLockMap.putIfAbsent(clientKey, newLock);
            if (lock == null) {
                lock = newLock;
            }
        }
        return lock;
    }
}
