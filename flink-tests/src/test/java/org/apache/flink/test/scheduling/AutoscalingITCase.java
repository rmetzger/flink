/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.scheduling;

import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assume.assumeTrue;

public class AutoscalingITCase extends TestLogger {

    private static final int NUMBER_SLOTS_PER_TASK_MANAGER = 2;
    private static final int INITIAL_NUMBER_TASK_MANAGERS = 1;

    private static final Configuration configuration = getAutoscalingConfiguration();

    @Rule
    public final MiniClusterResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(configuration)
                            .setNumberTaskManagers(INITIAL_NUMBER_TASK_MANAGERS)
                            .setNumberSlotsPerTaskManager(NUMBER_SLOTS_PER_TASK_MANAGER)
                            .build());

    private static Configuration getAutoscalingConfiguration() {
        final Configuration conf = new Configuration();
        conf.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
        conf.set(RestOptions.BIND_PORT, "8081");

        return conf;
    }

    @Before
    public void assumeDeclarativeResourceManagement() {
        assumeTrue(ClusterOptions.isDeclarativeResourceManagementEnabled(configuration));
    }

    /** Test that a job scales up when a TaskManager gets added to the cluster. */
    @Test
    public void testScaleUpOnAdditionalTaskManager() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<String> input =
                env.addSource(new ReactiveModeITCase.ParallelismTrackingSource());
        input.addSink(new ReactiveModeITCase.ParallelismTrackingSink<>());

        ReactiveModeITCase.ParallelismTrackingSource.expectInstances(
                NUMBER_SLOTS_PER_TASK_MANAGER * INITIAL_NUMBER_TASK_MANAGERS);
        ReactiveModeITCase.ParallelismTrackingSink.expectInstances(
                NUMBER_SLOTS_PER_TASK_MANAGER * INITIAL_NUMBER_TASK_MANAGERS);

        env.executeAsync();

        ReactiveModeITCase.ParallelismTrackingSource.waitForInstances();
        ReactiveModeITCase.ParallelismTrackingSink.waitForInstances();

        // expect scale up to 2 TaskManagers:
        ReactiveModeITCase.ParallelismTrackingSource.expectInstances(
                NUMBER_SLOTS_PER_TASK_MANAGER * 2);
        ReactiveModeITCase.ParallelismTrackingSink.expectInstances(
                NUMBER_SLOTS_PER_TASK_MANAGER * 2);

        miniClusterResource.getMiniCluster().startTaskManager();

        ReactiveModeITCase.ParallelismTrackingSource.waitForInstances();
        ReactiveModeITCase.ParallelismTrackingSink.waitForInstances();
    }
}
