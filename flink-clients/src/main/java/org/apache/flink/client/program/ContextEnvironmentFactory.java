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

package org.apache.flink.client.program;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.runtime.yarn.AbstractFlinkYarnCluster;

import java.net.URL;
import java.util.List;

/**
 * The factory that instantiates the environment to be used when running jobs that are
 * submitted through a pre-configured client connection.
 * This happens for example when a job is submitted from the command line.
 */
public class ContextEnvironmentFactory implements ExecutionEnvironmentFactory {

	private final Client client;

	private final List<URL> jarFilesToAttach;

	private final List<URL> classpathsToAttach;

	private final ClassLoader userCodeClassLoader;

	private final int defaultParallelism;

	private final boolean wait;
	private final AbstractFlinkYarnCluster yarnCluster;

	private ExecutionEnvironment lastEnvCreated;


	public ContextEnvironmentFactory(Client client, List<URL> jarFilesToAttach,
			List<URL> classpathsToAttach, ClassLoader userCodeClassLoader, int defaultParallelism,
			boolean wait, AbstractFlinkYarnCluster yarnCluster)
	{
		this.client = client;
		this.jarFilesToAttach = jarFilesToAttach;
		this.classpathsToAttach = classpathsToAttach;
		this.userCodeClassLoader = userCodeClassLoader;
		this.defaultParallelism = defaultParallelism;
		this.wait = wait;
		this.yarnCluster = yarnCluster;
	}

	@Override
	public ExecutionEnvironment createExecutionEnvironment() {
		if (!wait && lastEnvCreated != null) {
			throw new InvalidProgramException("Multiple enviornments cannot be created in detached mode");
		}

		lastEnvCreated = wait ?
				new ContextEnvironment(client, jarFilesToAttach, classpathsToAttach, userCodeClassLoader, yarnCluster) :
				new DetachedEnvironment(client, jarFilesToAttach, classpathsToAttach, userCodeClassLoader);
		if (defaultParallelism > 0) {
			lastEnvCreated.setParallelism(defaultParallelism);
		}
		return lastEnvCreated;
	}

	public ExecutionEnvironment getLastEnvCreated() {
		return lastEnvCreated;
	}
}
