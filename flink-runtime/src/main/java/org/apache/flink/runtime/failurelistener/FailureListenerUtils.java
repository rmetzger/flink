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

package org.apache.flink.runtime.failurelistener;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.failurelistener.FailureListener;
import org.apache.flink.core.failurelistener.FailureListenerFactory;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** Utils for creating failure listener. */
public class FailureListenerUtils {

    public static List<FailureListener> getFailureListerners(
            Configuration configuration, JobManagerJobMetricGroup metricGroup) {
        PluginManager pluginManager = PluginUtils.createPluginManagerFromRootFolder(configuration);
        Iterator<FailureListenerFactory> fromPluginManager =
                pluginManager.load(FailureListenerFactory.class);

        List<FailureListener> failureListeners = new ArrayList<>();
        DefaultFailureListener defaultFailureListener = new DefaultFailureListener();
        defaultFailureListener.init(metricGroup.jobId(), metricGroup.jobName(), metricGroup);
        failureListeners.add(defaultFailureListener);
        while (fromPluginManager.hasNext()) {
            FailureListenerFactory failureListenerFactory = fromPluginManager.next();
            FailureListener failureListener =
                    failureListenerFactory.createFailureListener(configuration);
            failureListener.init(metricGroup.jobId(), metricGroup.jobName(), metricGroup);
            failureListeners.add(failureListener);
        }

        return failureListeners;
    }
}
