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

package cn.todd.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * Date: 2020/3/9
 * Company: www.dtstack.com
 * @author maqi
 */
public class KafkaReader {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        String jobName = params.get("jobName");
        String topic = params.get("topic");
        String bootstrapServers = params.get("bootstrapServers");

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServers);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        TypeInformation[] types = new TypeInformation[3];
        types[0] = Types.STRING;
        types[1] = Types.STRING;
        types[2] = Types.STRING;
        TypeInformation<Row> typeInformation = new RowTypeInfo(types, new String[]{"id", "name", "city"});

        FlinkKafkaConsumer<Row> consumer11 = new FlinkKafkaConsumer<Row>(topic,
                new MqJsonRowDeserializationSchema(typeInformation, false),
                props);

        consumer11.setStartFromLatest();

        DataStreamSource<Row> kafkaDataStream = env.addSource(consumer11);

        kafkaDataStream.print();

        env.execute(jobName);
    }
}
