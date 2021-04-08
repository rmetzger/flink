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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherJob;

/**
 * Listener for tracking the lifecycle of the JobManager by the {@link Dispatcher} (more
 * specifically by the {@link DispatcherJob}.
 */
public interface JobManagerStatusListener {
    /**
     * Notification that the JobManager has been successfully started.
     *
     * @param jobManagerRunner Instance of the JobManagerRunner that has been started.
     */
    void onJobManagerStarted(JobManagerRunner jobManagerRunner);

    /** Notification that the JobManager has been stopped. */
    void onJobManagerStopped();

    /**
     * Notification that the JobManager initialization has failed.
     *
     * @param initializationFailure Initialization failure cause.
     */
    void onJobManagerInitializationFailed(Throwable initializationFailure);
}
