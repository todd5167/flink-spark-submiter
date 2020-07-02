 IDEA直接提交Flink/Spark任务到集群，进行快速开发调试。任务提交代码稍加改造后可以和上层调度系统进行集成。
 
 
 任务类型提交入口类：
 
 [Spark-Yarn-Submit-Client](spark-yarn-submiter/src/main/java/cn/todd/spark/launcher/LauncherMain.java)
 
 [Spark-K8s-Submit-Client](spark-k8s-submiter/src/main/java/cn/todd/spark/launcher/LauncherMain.java)
 
 [Flink-Yarn-Submit-Client](flink-yarn-submiter/src/main/java/cn/todd/flink/launcher/LauncherMain.java)
 
 
 
 #### Flink 多执行模式任务提交
 - 需要填写Flink任务运行时参数配置，任务运行所在的集群配置路径，本地Flink根路径。项目依赖flink1.10版本。
 - 支持以YarnSession、YarnPerjob、Standalone模式进行任务提交，返回ApplicationId。
 - example模块下包含一个FlinkDemo，打包后会转移到项目的examplJars中，可以尝试进行任务提交。
 - 任务提交后，根据ApplicationId获取任务执行使用的jm、tm日志基本信息，包含日志访问URL,日志总字节大小,根据日志基本信息可以做日志滚动展示，防止Yarn日志过大导致日志读取卡死。

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
    List<String> logsInfo = new RunningLog().getRollingLogBaseInfo(jobParamsInfo, applicationId);
    logsInfo.forEach(System.out::println);
 ```
 
jobmanager日志格式:
   
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