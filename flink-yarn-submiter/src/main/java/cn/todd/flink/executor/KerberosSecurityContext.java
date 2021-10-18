package cn.todd.flink.executor;

import org.apache.flink.util.StringUtils;

import cn.todd.flink.ClusterClient;
import cn.todd.flink.entity.ParamsInfo;
import cn.todd.flink.factory.YarnClusterDescriptorFactory;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.kerby.kerberos.kerb.keytab.Keytab;
import org.apache.kerby.kerberos.kerb.type.base.PrincipalName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * executor function in kerberos env Date: 2021/10/1
 *
 * @author todd5167
 */
public class KerberosSecurityContext {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterClient.class);

    private static Cache<String, UserGroupInformation> ugiCache =
            CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.HOURS).build();

    public static <T> T runSecured(ParamsInfo jobParamsInfo, final Supplier<T> supplier)
            throws IOException, InterruptedException {
        LOG.info("KerberosSecurityContext jobParamsInfo:{}", jobParamsInfo.toString());

        String krb5Path = jobParamsInfo.getKrb5Path();
        String principal = jobParamsInfo.getPrincipal();
        String keytabPath = jobParamsInfo.getKeytabPath();
        String hadoopConfDir = jobParamsInfo.getHadoopConfDir();

        if (StringUtils.isNullOrWhitespaceOnly(principal)) {
            principal = extractPrincipalFromKeytab(keytabPath);
        }

        String cacheKey = hadoopConfDir + "_" + principal;
        UserGroupInformation cachedUgi = ugiCache.getIfPresent(cacheKey);
        if (Objects.nonNull(cachedUgi)) {
            return cachedUgi.doAs((PrivilegedExceptionAction<T>) supplier::get);
        }

        if (!StringUtils.isNullOrWhitespaceOnly(krb5Path)) {
            System.setProperty("java.security.krb5.conf", krb5Path);
        }

        YarnConfiguration yarnConf =
                YarnClusterDescriptorFactory.INSTANCE.parseYarnConfFromConfDir(
                        jobParamsInfo.getHadoopConfDir());

        // print auth_to_local
        String auth_to_local = yarnConf.get("hadoop.security.auth_to_local");
        LOG.debug("auth_to_local is : {}", auth_to_local);
        // security context init
        UserGroupInformation.setConfiguration(yarnConf);
        UserGroupInformation ugi =
                UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytabPath);

        LOG.info(
                "userGroupInformation current user = {} ugi user  = {} ",
                UserGroupInformation.getCurrentUser(),
                ugi.getUserName());

        if (jobParamsInfo.isCacheUgi()) {
            // Cache UGI requires a thread to correspond to a principal
            ugiCache.put(cacheKey, ugi);
        }
        return ugi.doAs((PrivilegedExceptionAction<T>) supplier::get);
    }

    private static String extractPrincipalFromKeytab(String keytabPath) throws IOException {
        Keytab keytab = Keytab.loadKeytab(new File(keytabPath));
        List<PrincipalName> principals = keytab.getPrincipals();
        principals.forEach(principalName -> LOG.info("principalName:{}", principalName));
        return principals.get(0).getName();
    }
}
