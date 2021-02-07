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

package cn.todd.flink.enums;

/**
 *  Flink任务运行模式
 * @author todd5167
 */
public enum ERunMode {
    /* 运行在已有的应用中，适合短期执行的任务 **/
    YARN_SESSION,

    /* 为Job新建应用，适合长期执行的任务 **/
    YARN_PERJOB,

    /* 避免从client上传文件，导致的网络IO开销 **/
    YARN_APPLICATION,

    STANDALONE;

    public static ERunMode convertFromString(String type) {
        if (type == null) {
            throw new RuntimeException("null RunMode!");
        }
        return valueOf(type.toUpperCase());
    }

}
