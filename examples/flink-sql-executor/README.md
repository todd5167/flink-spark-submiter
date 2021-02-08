仿照datagen,print插件实现的randomdata,console插件。执行时通过sqlFilePath参数，指定sql文件的位置。


flink sql 样例：

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