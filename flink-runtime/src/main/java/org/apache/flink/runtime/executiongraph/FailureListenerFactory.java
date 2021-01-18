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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** Factory class for creating {@link FailureListener} with plugin Manager. */
public class FailureListenerFactory {
    private final PluginManager pluginManager;
    private List<FailureListener> failureListeners = new ArrayList<>();

    public FailureListenerFactory(
            Configuration configuration, JobManagerJobMetricGroup metricGroup) {
        this.pluginManager = PluginUtils.createPluginManagerFromRootFolder(configuration);
        Iterator<FailureListener> fromPluginManager = pluginManager.load(FailureListener.class);
        failureListeners.add(new DefaultFailureListener());

        while (fromPluginManager.hasNext()) {
            FailureListener failureListener = fromPluginManager.next();
            failureListener.init(metricGroup);
            failureListeners.add(failureListener);
        }
    }

    public List<FailureListener> createFailureListener() {
        return failureListeners;
    }
}
