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

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.util.ResourceCounter;

public class ReactiveModeScalingPolicy implements ScalingPolicy {
    @Override
    public SchedulerConfiguration getSchedulerConfiguration() {
        return new ReactiveSchedulerConfiguration();
    }

    @Override
    public void scalingCallback(ScalingContext context) {}

    @Override
    public boolean canScaleUp(int currentCumulativeParallelism, int newCumulativeParallelism) {
        return false;
    }

    private static class ReactiveSchedulerConfiguration implements SchedulerConfiguration {
        @Override
        public int getScalingCallbackFrequencySeconds() {
            return -1;
        }

        @Override
        public ResourceCounter getInitialDesiredResources() {
            return ResourceCounter.withResource(ResourceProfile.ANY, 30000);
        }
    }
}
