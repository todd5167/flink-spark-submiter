 
Flink任务、Spark任务提交到集群，通常需要将可执行Jar上传到集群，手动执行任务提交指令，如果有配套的大数据平台则需要上传Jar，由调度系统进行任务提交。
对开发者来说，本地IDEA调试Flink、Spark任务不涉及对象的序列化及反序列化，任务在本地调试通过后，执行在分布式环境下也可能会出错。
而将任务提交到集群进行调试还要走那些繁琐的流程太影响效率了。

因此，为方便大数据开发人员进行快速开发调试，开发了从本地IDEA提交Flink/Spark任务到集群的工具类。任务提交代码稍加改造后也可以和上层调度系统进行集成，替代脚本模式进行任务提交的方式。

- 支持Flink yarnPerJob、Standalone 、yarnSession模式下的任务提交。

- 支持Spark任务以Yarn Cluster模式提交到YARN，支持自动上传用户Jar包，依赖的Spark Jars需要提前上传到HDFS。

- 支持Spark任务提交到K8s Cluster，执行的jar需要包含在镜像中，任务执行时需要传递镜像名称及可执行文件路径。如果需要操作hive表，则需要传递集群所在文件夹，以及HADOOP_USER_NAME，系统进行Hadoop文件的挂载及环境变量的设置。
 
 
 
 #### 任务类型提交入口类：
 
 [Spark-Yarn-Submit-Client](spark-yarn-submiter/src/main/java/cn/todd/spark/launcher/LauncherMain.java)
 
 [Spark-K8s-Submit-Client](spark-k8s-submiter/src/main/java/cn/todd/spark/launcher/LauncherMain.java)
 
 [Flink-Yarn-Submit-Client](flink-yarn-submiter/src/main/java/cn/todd/flink/launcher/LauncherMain.java)
 
 
 
 #### Flink 多执行模式任务提交
 - 需要填写Flink任务运行时参数配置，任务运行所在的集群配置路径，本地Flink根路径。项目依赖flink1.10版本。
 - 支持以YarnSession、YarnPerjob、Standalone模式进行任务提交，返回ApplicationId。
 - example模块下包含一个FlinkDemo，打包后会转移到项目的examplJars中，可以尝试进行任务提交。
 - 任务提交后，根据ApplicationId获取任务执行使用的jm、tm日志基本信息，包含日志访问URL,日志总字节大小,根据日志基本信息可以做日志滚动展示，防止Yarn日志过大导致日志读取卡死。
 - 提供任务取消、任务状态获取、已完成任务日志获取接口。

任务提交示例：
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
     //        String yarnConfDir = "/Users/maqi/tmp/hadoopconf";
     String yarnConfDir = "/Users/maqi/tmp/hadoopconf";
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

 
    String applicationId = runFlinkJob(jobParamsInfo, execMode);
    //任务启动后，拉取jm,tm日志相关信息。
    Thread.sleep(20000);
    List<String> logsInfo = new RunningLog().getRollingLogBaseInfo(jobParamsInfo, applicationId);
    logsInfo.forEach(System.out::println);
 ```
 
jobmanager日志格式:
   
 ```aidl
{
    "logs":[
        {
            "name":"jobmanager.err ",
            "totalBytes":"555",
            "url":"http://172-16-10-204:8042/node/containerlogs/container_e185_1593317332045_2246_01_000002/admin/jobmanager.err/"
        },
        {
            "name":"jobmanager.log ",
            "totalBytes":"31944",
            "url":"http://172-16-10-204:8042/node/containerlogs/container_e185_1593317332045_2246_01_000002/admin/jobmanager.log/"
        },
        {
            "name":"jobmanager.out ",
            "totalBytes":"0",
            "url":"http://172-16-10-204:8042/node/containerlogs/container_e185_1593317332045_2246_01_000002/admin/jobmanager.out/"
        }
    ],
    "typeName":"jobmanager"
}
```

taskmanager日志格式:
```aidl
{
    "logs":[
        {
            "name":"taskmanager.err ",
            "totalBytes":"560",
            "url":"http://node03:8042/node/containerlogs/container_e27_1593571725037_0170_01_000002/admin/taskmanager.err/"
        },
        {
            "name":"taskmanager.log ",
            "totalBytes":"35937",
            "url":"http://node03:8042/node/containerlogs/container_e27_1593571725037_0170_01_000002/admin/taskmanager.log/"
        },
        {
            "name":"taskmanager.out ",
            "totalBytes":"0",
            "url":"http://node03:8042/node/containerlogs/container_e27_1593571725037_0170_01_000002/admin/taskmanager.out/"
        }
    ],
    "otherInfo":"{"dataPort":36218,"freeSlots":0,"hardware":{"cpuCores":4,"freeMemory":241172480,"managedMemory":308700779,"physicalMemory":8201641984},"id":"container_e27_1593571725037_0170_01_000002","path":"akka.tcp://flink@node03:36791/user/taskmanager_0","slotsNumber":1,"timeSinceLastHeartbeat":1593659561129}",
    "typeName":"taskmanager"
}
```
 
 #### Spark on yarn 任务提交
- 填写用户程序包路径、执行参数、集群配置文件夹、安全认证等相关配置。
- Spark任务提交使用Yarn cluster模式，使用的Spark Jar需要提前上传到HDFS并作为archive的参数。
- 针对SparkSQL任务，通过提交examples中的spark-sql-proxy程序包来直接操作hive表。

提交示例：
 ```aidl
 public static void main(String[] args) throws Exception {
         boolean openKerberos = true;
         String appName = "todd spark submit";
         String runJarPath = "/Users/maqi/code/ClustersSubmiter/exampleJars/spark-sql-proxy/spark-sql-proxy.jar";
         String mainClass = "cn.todd.spark.SparksqlProxy";
         String yarnConfDir = "/Users/maqi/tmp/hadoopconf";
         String principal = "hdfs/node1@DTSTACK.COM";
         String keyTab = "/Users/maqi/tmp/hadoopconf/hdfs.keytab";
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
 
  #### Spark on k8s 任务提交
  
  - 基于Spark2.4.4进行开发，通过将spark-sql-proxy.jar包打入镜像来执行Sparksql并操作Hive表，无其他特殊操作。
  - 操作Hive时需要传递hadoopConfDir,程序会自动将.xml文件内容进行挂载，如果非root用户操作Hive,需要设置HADOOP_USER_NAME。
  - 通过读取kubeConfig配置文件进行Kuberclient的创建,而非官方提供的master url方式。
  - 任务提交后立即返回spark-app-selector id,从而进行POD状态获取。
  
  ```aidl
    public static void main(String[] args) throws Exception {
        String appName = "todd spark submit";
        // 镜像内的jar路径
        String runJarPath = "local:///opt/dtstack/spark/spark-sql-proxy.jar";
        String mainClass = "cn.todd.spark.SparksqlProxy";
        String hadoopConfDir = "/Users/maqi/tmp/hadoopconf/";
        String kubeConfig = "/Users/maqi/tmp/conf/k8s.config";
        String imageName = "mqspark:2.4.4";
        String execArgs = getExampleJobParams();

        Properties confProperties = new Properties();
        confProperties.setProperty("spark.executor.instances", "2");
        confProperties.setProperty("spark.kubernetes.namespace", "default");
        confProperties.setProperty("spark.kubernetes.authenticate.driver.serviceAccountName", "spark");
        confProperties.setProperty("spark.kubernetes.container.image.pullPolicy", "IfNotPresent");


        JobParamsInfo jobParamsInfo = JobParamsInfo.builder()
                .setAppName(appName)
                .setRunJarPath(runJarPath)
                .setMainClass(mainClass)
                .setExecArgs(execArgs)
                .setConfProperties(confProperties)
                .setHadoopConfDir(hadoopConfDir)
                .setKubeConfig(kubeConfig)
                .setImageName(imageName)
                .build();

        String id = run(jobParamsInfo);
        System.out.println(id);
    }
```