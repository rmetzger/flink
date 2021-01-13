/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.declarative;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testtasks.OnceBlockingNoOpInvokable;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.testutils.junit.WithDeclarativeScheduler;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * This class contains integration tests for the declarative scheduler which start a {@link
 * org.apache.flink.runtime.minicluster.MiniCluster} per test case.
 */
@Category(WithDeclarativeScheduler.class)
public class DeclarativeSchedulerClusterITCase extends TestLogger {

    private static final int NUMBER_SLOTS_PER_TASK_MANAGER = 2;
    private static final int NUMBER_TASK_MANAGERS = 2;
    private static final int PARALLELISM = NUMBER_SLOTS_PER_TASK_MANAGER * NUMBER_TASK_MANAGERS;

    private final Configuration configuration = createConfiguration();

    @Rule
    public final MiniClusterResource miniClusterResource =
            new MiniClusterResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(configuration)
                            .setNumberSlotsPerTaskManager(NUMBER_SLOTS_PER_TASK_MANAGER)
                            .setNumberTaskManagers(NUMBER_TASK_MANAGERS)
                            .build());

    private Configuration createConfiguration() {
        final Configuration configuration = new Configuration();

        configuration.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Declarative);
        configuration.set(ClusterOptions.ENABLE_DECLARATIVE_RESOURCE_MANAGEMENT, true);

        return configuration;
    }

    @Test
    public void testAutomaticScaleDownInCaseOfLostSlots() throws InterruptedException, IOException {
        assumeTrue(ClusterOptions.isDeclarativeResourceManagementEnabled(configuration));

        final MiniCluster miniCluster = miniClusterResource.getMiniCluster();
        final JobGraph jobGraph = createBlockingJobGraph(PARALLELISM);

        miniCluster.submitJob(jobGraph).join();
        final CompletableFuture<JobResult> resultFuture =
                miniCluster.requestJobResult(jobGraph.getJobID());

        OnceBlockingNoOpInvokable.waitUntilOpsAreRunning();

        miniCluster.terminateTaskManager(0);

        final JobResult jobResult = resultFuture.join();

        assertTrue(jobResult.isSuccess());
    }

    @Test
    public void testAutomaticScaleUp() throws Exception {
        assumeTrue(ClusterOptions.isDeclarativeResourceManagementEnabled(configuration));

        final MiniCluster miniCluster = miniClusterResource.getMiniCluster();
        final JobGraph jobGraph = createBlockingJobGraph(PARALLELISM);

        log.info("Submitting job with parallelism of " + PARALLELISM);
        miniCluster.submitJob(jobGraph).join();

        OnceBlockingNoOpInvokable.waitUntilOpsAreRunning();

        log.info("Start additional TaskManager");
        miniCluster.startTaskManager();

        log.info("Waiting until Invokable is running with higher parallelism");
        int targetInstanceCount = NUMBER_SLOTS_PER_TASK_MANAGER * (NUMBER_TASK_MANAGERS + 1);
        while (OnceBlockingNoOpInvokable.getInstanceCount() < targetInstanceCount) {
            log.info("instance count " + OnceBlockingNoOpInvokable.getInstanceCount());
            Thread.sleep(50);
        }
        log.info("final instance count " + OnceBlockingNoOpInvokable.getInstanceCount());
        assertEquals(targetInstanceCount, OnceBlockingNoOpInvokable.getInstanceCount());
        miniCluster.cancelJob(jobGraph.getJobID());
    }

    private JobGraph createBlockingJobGraph(int parallelism) throws IOException {
        final JobVertex blockingOperator = new JobVertex("Blocking operator");

        OnceBlockingNoOpInvokable.resetFor(parallelism);
        blockingOperator.setInvokableClass(OnceBlockingNoOpInvokable.class);

        blockingOperator.setParallelism(parallelism);

        final JobGraph jobGraph = new JobGraph("Blocking job.", blockingOperator);

        ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L));
        jobGraph.setExecutionConfig(executionConfig);

        return jobGraph;
    }
}
