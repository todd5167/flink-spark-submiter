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
 *
 * Date: 2020/7/15
 * @author todd5167
 */
public enum ETaskStatus {
    //
    CREATED(1),
    //
    ACCEPTED(2),
    //
    SUBMITTED(3),
    //
    RUNNING(4),
    //
    KILLED(5),
    //
    FAILED(6),
    //
    FINISHED(7),
    //
    NOTFOUND(8),
    //
    FAILING(9),
    //
    CANCELLING(10),
    //
    CANCELED(11),
    //
    RESTARTING(12),
    //
    SUSPENDED(13),
    //
    RECONCILING(14);

    private int status;

    ETaskStatus(int status) {
        this.status = status;
    }

    public Integer getStatus() {
        return this.status;
    }

}
