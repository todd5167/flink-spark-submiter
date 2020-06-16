 IDEA直接提交Flink/Spark任务到集群，方便调试。
 
 [Spark-Yarn-Submit-Client](spark-yarn-submiter/src/main/java/cn/todd/spark/launcher/LauncherMain.java)
 
 [Spark-K8s-Submit-Client](spark-k8s-submiter/src/main/java/cn/todd/spark/LauncherMain.java)
 
 [Flink-Yarn-Submit-Client](flink-yarn-submiter/src/main/java/cn/todd/flink/launcher/LauncherMain.java)
 
 
 #### Flink任务提交示例
 通过填写Flink任务执行时需要的参数信息，任务执行模式，以及集群文件本地即可进行任务提交。任务提交后，会返回任务执行使用的jm、tm日志基本信息，后期会做动态日志拉取，滚动获取日志。
 ```
        // 可执行jar包路径
         String runJarPath = "/Users/maqi/code/ClustersSubmiter/exampleJars/flink-kafka-reader/flink-kafka-reader.jar";
         // 任务参数
         String[] execArgs = new String[]{"-jobName", "flink110Submit", "--topic", "mqTest01", "--bootstrapServers", "172.16.8.107:9092"};
         // 任务名称
         String jobName = "Flink perjob submit";
         // flink 文件夹路径
         String flinkConfDir = "/Users/maqi/tmp/flink/flink-1.10.0/conf";
         // flink lib包路径
         String flinkJarPath = "/Users/maqi/tmp/flink/flink-1.10.0/lib";
         //  yarn 文件夹路径
         //        String yarnConfDir = "/Users/maqi/tmp/__spark_conf__6181052549606078780";
         String yarnConfDir = "/Users/maqi/tmp/hadoopconf/195";
         //  作业依赖的外部文件，例如：udf jar , keytab
         String[] dependFile = new String[]{"/Users/maqi/tmp/flink/flink-1.10.0/README.txt"};
         // 任务提交队列
         String queue = "root.users.hdfs";
         // flink任务执行模式
         String execMode = "yarnPerjob";
         // yarnsession appid配置
         Properties yarnSessionConfProperties = null;
         // savepoint 及并行度相关
         Properties confProperties = new Properties();
         confProperties.setProperty("parallelism", "1");
 
       JobParamsInfo jobParamsInfo = JobParamsInfo.builder()
                 .setExecArgs(execArgs)
                 .setName(jobName)
                 .setRunJarPath(runJarPath)
                 .setDependFile(dependFile)
                 .setFlinkConfDir(flinkConfDir)
                 .setYarnConfDir(yarnConfDir)
                 .setConfProperties(confProperties)
                 .setYarnSessionConfProperties(yarnSessionConfProperties)
                 .setFlinkJarPath(flinkJarPath)
                 .setQueue(queue)
                 .build();
 
      runFlinkJob(jobParamsInfo, execMode);
 ```
 
 #### Spark任务提交示例
 Spark on yarn与Flink任务提交类似，不过需要提前将Spark使用的类库上传到HDFS从而减少文件传输数量。任务提交时，会从yarnConfDir下查找集群使用的文件配置。
 
 ```
 public static void main(String[] args) throws Exception {
         boolean openKerberos = true;
         String appName = "todd spark submit";
         String runJarPath = "/Users/maqi/code/ClustersSubmiter/exampleJars/spark-sql-proxy/spark-sql-proxy.jar";
         String mainClass = "cn.todd.spark.SparksqlProxy";
         String yarnConfDir = "/Users/maqi/tmp/__spark_conf__6181052549606078780";
         String principal = "hdfs/node1@DTSTACK.COM";
         String keyTab = "/Users/maqi/tmp/hadoopconf/cdh514/hdfs.keytab";
         String jarHdfsDir = "sparkproxy2";
         String archive = "hdfs://nameservice1/sparkjars/jars";
         String queue = "root.users.hdfs";
         String execArgs = getExampleJobParams();
 
         Properties confProperties = new Properties();
         confProperties.setProperty("spark.executor.cores","2");
 
         JobParamsInfo jobParamsInfo = JobParamsInfo.builder()
                 .setAppName(appName)
                 .setRunJarPath(runJarPath)
                 .setMainClass(mainClass)
                 .setYarnConfDir(yarnConfDir)
                 .setPrincipal(principal)
                 .setKeytab(keyTab)
                 .setJarHdfsDir(jarHdfsDir)
                 .setArchivePath(archive)
                 .setQueue(queue)
                 .setExecArgs(execArgs)
                 .setConfProperties(confProperties)
                 .setOpenKerberos(BooleanUtils.toString(openKerberos, "true", "false"))
                 .build();
 
         YarnConfiguration yarnConf = YarnConfLoaderUtil.getYarnConf(yarnConfDir);
         String applicationId = "";
 
         if (BooleanUtils.toBoolean(openKerberos)) {
             UserGroupInformation.setConfiguration(yarnConf);
             UserGroupInformation userGroupInformation = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keyTab);
             applicationId = userGroupInformation.doAs((PrivilegedExceptionAction<String>) () -> LauncherMain.run(jobParamsInfo, yarnConf));
         } else {
             LauncherMain.run(jobParamsInfo, yarnConf);
         }
 
         System.out.println(applicationId);
     }
 ```