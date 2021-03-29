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

package org.apache.flink.runtime.scheduler.adaptive.scalingpolicy;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.util.ResourceCounter;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Noop. */
public class CPUAutoscalingPolicy implements ScalingPolicy {
    private static final Logger LOG = LoggerFactory.getLogger(CPUAutoscalingPolicy.class);

    private static final ObjectMapper mapper = new ObjectMapper();
    private final float scaleUpOn;
    private final float scaleDownOn;

    public CPUAutoscalingPolicy(Configuration config) {
        scaleUpOn = config.getFloat("autoscaling.scaleUpOn", 0.4f);
        scaleDownOn = config.getFloat("autoscaling.scaleDownOn", 0.01f);
    }

    @Override
    public SchedulerConfiguration getSchedulerConfiguration() {
        return new CpuAutoscalingSchedulerConfiguration();
    }

    @Override
    public void scalingCallback(ScalingContext context) {
        double cpuLoad = getCpuLoad();
        if (cpuLoad < 0) {
            return;
        }

        LOG.info("CPU load is {}", cpuLoad);
        if (cpuLoad > scaleUpOn) {
            context.setDesiredResources(getResourceCounter(context.getTotalSlots() + 1));
            LOG.info("Increase desired slot count to {}", context.getTotalSlots() + 1);
        }

        if (cpuLoad < scaleDownOn) {
            int desiredSlots = context.getTotalSlots() - 1;
            if (desiredSlots < 1) {
                desiredSlots = 1;
            }
            context.setDesiredResources(getResourceCounter(desiredSlots));
            LOG.info("Decrease desired slot count to {}", desiredSlots);
        }
    }

    @Override
    public boolean canScaleUp(int currentCumulativeParallelism, int newCumulativeParallelism) {
        return true;
    }

    private static ResourceCounter getResourceCounter(int slots) {
        return ResourceCounter.withResource(ResourceProfile.UNKNOWN, slots);
    }

    // todo consider using MetricFetcherImpl, see also AbstractAggregatingMetricsHandler
    private static double getCpuLoad() {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpGet request =
                    new HttpGet(
                            "http://localhost:8081/taskmanagers/metrics?get=Status.JVM.CPU.Load");

            ArrayNode response =
                    client.execute(
                            request,
                            httpResponse ->
                                    mapper.readValue(
                                            httpResponse.getEntity().getContent(),
                                            ArrayNode.class));

            return response.get(0).get("avg").asDouble();
        } catch (Throwable e) {
            LOG.info("Error while fetching CPU load", e);
            return -1;
        }
    }

    private static class CpuAutoscalingSchedulerConfiguration implements SchedulerConfiguration {
        @Override
        public int getScalingCallbackFrequencySeconds() {
            return 10;
        }

        @Override
        public ResourceCounter getInitialDesiredResources() {
            return getResourceCounter(1);
        }
    }
}
