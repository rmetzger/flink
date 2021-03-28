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

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.util.ResourceCounter;

import java.util.Map;

/** TODO: add reactive mode policy to validate. */
public interface ScalingPolicy {
    // this method gets initially called by the scheduler for some configurations.
    SchedulerConfiguration getSchedulerConfiguration();

    // periodic call by the scheduler to the policy, to see if a scaling is required
    // method gets called immediately if new slots are available
    void scalingCallback(ScalingContext context);

    boolean canScaleUp(int currentCumulativeParallelism, int newCumulativeParallelism);

    /**
     * provides the policy with the necessary data and interaction to try different scales, or
     * commit to one.
     */
    interface ScalingContext {
        int getFreeSlots();

        int getTotalSlots();

        // ask the SlotAllocator to compute a ScalingProposal for a given number of slots
        ScalingProposal computeScale(int slots) throws SlotsExceededException;

        // instruct scheduler to rescale to the provided number of slots.
        void setDesiredResources(ResourceCounter desiredResources);
    }

    /** yolo. */
    interface SchedulerConfiguration {
        int getScalingCallbackFrequencySeconds();

        ResourceCounter getInitialDesiredResources();
    }

    /** yolo. */
    interface ScalingProposal {
        Map<TaskInformation, Integer> getCurrentVertexParallelisms();

        Map<TaskInformation, Integer> getProposedVertexParallelisms();
    }

    /** yolo. */
    interface TaskInformation {
        int getOriginalParallelism();

        int getMaxParallelism();

        int getName();

        JobVertexID getId();
    }

    /** yolo. */
    class SlotsExceededException extends Exception {}
}
