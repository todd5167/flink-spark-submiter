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
 * Date: 2020/10/1
 *
 * @author todd5167
 */
public enum ETaskStatus {

    /** Application has been accepted by the scheduler */
    ACCEPTED(23, "调度中"),

    /** Application which is currently running. */
    RUNNING(24, "运行中"),

    /** Application which finished successfully. */
    SUCCEEDED(25, "成功"),

    /** Application which failed. */
    FAILED(26, "失败"),

    /** Application which was terminated by a user or admin. */
    KILLED(27, "杀死"),

    /** Application which not found . */
    NOT_FOUND(28, "异常");

    private int value;
    private String desc;

    ETaskStatus(int value, String desc) {
        this.value = value;
        this.desc = desc;
    }

    public int getValue() {
        return value;
    }

    public String getDesc() {
        return desc;
    }
}
