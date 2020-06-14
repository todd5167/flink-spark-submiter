/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.todd.spark;

import com.google.common.base.Charsets;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.List;
import java.util.Map;
import java.util.Optional;


/**
 *
 *  执行Spark sql的代理类
 *  Date: 2020/6/14
 * @author maqi
 */
public class SparksqlProxy {
    private static final ObjectMapper objMapper = new ObjectMapper();
    private static final String APP_NAME_KEY = "appName";
    private static final String APP_DEFAULT_NAME = "spark_sql_default_name";
    private static final String SQL_KEY = "sql";
    private static final String SPARK_CONF_KEY = "sparkSessionConf";

    /**
     *
     * 1.sql: 执行的多条SQL语句，以分号进行分割
     * 2.appName：Spark任务名称
     * 3.spark conf: 传递的spark配置
     *
     * @param args
     */
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            throw new RuntimeException("args must not null!!");
        }
        String argsJson = URLDecoder.decode(args[0], Charsets.UTF_8.name());
        Map<String, Object> argsMap = objMapper.readValue(argsJson, Map.class);

        String appName = (String) argsMap.getOrDefault(APP_NAME_KEY, APP_DEFAULT_NAME);
        String multipleSql = (String) argsMap.get(SQL_KEY);
        SparkConf sparkConf = generateSparkConf(argsMap.get(SPARK_CONF_KEY));

        SparksqlProxy sparkProxy = new SparksqlProxy();
        sparkProxy.runSql(multipleSql, appName, sparkConf);

    }

    /**
     *
     * @param sql
     * @param appName
     * @param sparkConf
     */
    private void runSql(String sql, String appName, SparkConf sparkConf) throws UnsupportedEncodingException {
        SparkSession sparkSession = SparkSession
                .builder()
                .config(sparkConf)
                .appName(appName)
                .enableHiveSupport()
                .getOrCreate();

        String encodeSql = URLDecoder.decode(sql, Charsets.UTF_8.name());
        List<String> sqlArray = StringUtils.splitIgnoreQuota(encodeSql, ';');

        sqlArray.stream()
                .filter(singleSql -> !(singleSql == null || singleSql.trim().length() == 0))
                .forEach(singleSql -> sparkSession.sql(singleSql));

        sparkSession.close();
    }

    private static SparkConf generateSparkConf(Object sparkConfArgs) {
        SparkConf sparkConf = new SparkConf();

        Optional.ofNullable(sparkConf).ifPresent(conf -> {
            Map<String, String> sparkSessionConf = (Map<String, String>) sparkConfArgs;
            sparkSessionConf.forEach((property, value) -> sparkConf.set(property, value));
        });

        return sparkConf;
    }
}
