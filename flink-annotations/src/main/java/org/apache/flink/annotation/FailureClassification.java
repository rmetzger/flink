/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.annotation;

/** Enum-like for test failure classifications. */
public class FailureClassification {
    public static final String REQUIRES_ATTEMPT_NUMBER = "Relies on attempt numbers";
    public static final String KV_OP_DURING_WAITING_FOR_RESOURCES =
            "Relies on KvState operations being available at all times";
    public static final String CHECKPOINT_DATA_READ_ON_STARTUP =
            "Relies on checkpoint data being read immediately on JM startup";
    public static final String SUBTASK_ACCESS_BEFORE_SCHEDULED =
            "Relies on subtasks being available right away";
    public static final String CHECKS_BEHAVIOR_WITH_DEFAULT_CONFIG =
            "Relies on certain options not being set.";
}
