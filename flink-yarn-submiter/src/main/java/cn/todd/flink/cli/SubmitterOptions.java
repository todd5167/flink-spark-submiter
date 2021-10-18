package cn.todd.flink.cli;

import org.apache.commons.cli.Option;

/**
 * Date: 2021/10/1
 *
 * @author todd5167
 */
public class SubmitterOptions {
    public static final Option NAME_OPTION =
            new Option("name", true, "flink作业名称，默认：flink submitter job.");

    public static final Option QUEUE_OPTION = new Option("queue", true, "flink作业名称，默认：default.");

    public static final Option RUN_JAR_PATH_OPTION =
            new Option("runJarPath", true, "待提交的flink jar 包路径.");

    public static final Option FLINK_CONF_DIR_OPTION =
            new Option("flinkConfDir", true, "flink conf文件所在路径.");

    public static final Option FLINK_JAR_PATH_OPTION =
            new Option("flinkJarPath", true, "flink lib所在路径.");

    public static final Option HADOOP_CONF_DIR_OPTION =
            new Option("hadoopConfDir", true, "hadoop配置文件所在路径.");

    public static final Option FLINK_VERSION_OPTION =
            new Option("flinkVersion", true, "flink版本号，service类加载使用");

    public static final Option ENTRY_POINT_CLASSNAME_OPTION =
            new Option("entryPointClassName", true, "待提交的flink jar包入口类");

    public static final Option EXEC_ARGS_OPTION =
            new Option("execArgs", true, "待提交的flink jar包接收的参数");

    public static final Option DEPEND_FILES_OPTION =
            new Option("dependFiles", true, "程序运行依赖的外部文件，例如：udf包");

    public static final Option FLINK_CONF_OPTION =
            new Option("confProperties", true, "为每个作业单独指定配置");

    public static final Option OPEN_SECURITY_OPTION =
            new Option("openSecurity", true, "是否开启Kerberos认证");

    public static final Option KRB5_PATH_OPTION = new Option("krb5Path", true, "krb5Path文件路径");

    public static final Option PRINCIPAL_OPTION = new Option("principal", true, "kerberos认证主体");

    public static final Option KEYTAB_PATH_OPTION =
            new Option("keytabPath", true, "kerberos认证使用的keytab文件");

    public static final Option APPLICATION_ID_OPTION =
            new Option("applicationId", true, "yarn application id");

    static {
        HADOOP_CONF_DIR_OPTION.setRequired(true);
    }
}
