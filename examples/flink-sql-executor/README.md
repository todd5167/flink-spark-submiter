- 仿照datagen,print插件实现的randomdata,console插件。
- 支持原生Flinksql执行。

相关参数：

- sqlText： 要执行的flink sql语句
- confProp： 
   可选参数:
    - early.trigger：开启window统计提前触发功能，单位为秒(填写正整数即可)，如：5。
    - sql.ttl.min: 最小过期时间,大于0的整数,如1d、1h(d\D:天,h\H:小时,m\M:分钟,s\s:秒)
    - sql.ttl.max: 最大过期时间,大于0的整数,如2d、2h(d\D:天,h\H:小时,m\M:分钟,s\s:秒),需同时设置最小时间,且比最小时间大5分钟
    - state.backend: 任务状态后端，可选为MEMORY,FILESYSTEM,ROCKSDB，默认为flinkconf中的配置。
    - state.checkpoints.dir: FILESYSTEM,ROCKSDB状态后端文件系统存储路径，例如：hdfs://ns1/tal/flink120/checkpoints。
    - state.backend.incremental: ROCKSDB状态后端是否开启增量checkpoint,默认为true。
    - sql.checkpoint.unalignedCheckpoints：是否开启Unaligned Checkpoint,不开启false,开启true。默认为true。
    - sql.env.parallelism: 默认并行度设置
    - sql.max.env.parallelism: 最大并行度设置
    - time.characteristic: 可选值[ProcessingTime|IngestionTime|EventTime]
    - sql.checkpoint.interval: 设置了该参数表明开启checkpoint(ms)
    - sql.checkpoint.mode: 可选值[EXACTLY_ONCE|AT_LEAST_ONCE]
    - sql.checkpoint.timeout: 生成checkpoint的超时时间(ms)
    - sql.max.concurrent.checkpoints: 最大并发生成checkpoint数
    - sql.checkpoint.cleanup.mode: 默认是不会将checkpoint存储到外部存储,[true(任务cancel之后会删除外部存储)|false(外部存储需要手动删除)]
    - flinkCheckpointDataURI: 设置checkpoint的外部存储路径,根据实际的需求设定文件路径,hdfs://, file://- 


执行参数：
"--sqlText", flinksql, "--confProp", "{}

flinksql参数内容：
```aidl
CREATE TABLE source_table (
   id INT,
   score INT,
   address STRING
) WITH (
    'connector'='randomdata',
    'rows-per-second'='2',
    'fields.id.kind'='sequence',
    'fields.id.start'='1',
    'fields.id.end'='100000',
    'fields.score.min'='1',
    'fields.score.max'='100',
    'fields.address.length'='10'
);

CREATE TABLE console_table (
     id INT,
     score INT,
     address STRING
) WITH (
    'connector' = 'console'
);


insert into console_table
  select id, score, address from source_table;
```