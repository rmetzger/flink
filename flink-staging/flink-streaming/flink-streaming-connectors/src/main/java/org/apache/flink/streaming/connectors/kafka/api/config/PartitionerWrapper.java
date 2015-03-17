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
package org.apache.flink.streaming.connectors.kafka.api.config;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Hacky wrapper to send an object instance through a Properties - map.
 */
public class PartitionerWrapper implements Partitioner {
	public final static String SERIALIZED_WRAPPER_NAME = "flink.kafka.wrapper.serialized";

	private Partitioner wrapped;
	public PartitionerWrapper(VerifiableProperties properties) {
		wrapped = (Partitioner) properties.props().get(SERIALIZED_WRAPPER_NAME);
	}

	@Override
	public int partition(Object value, int numberOfPartitions) {
		return wrapped.partition(value, numberOfPartitions);
	}
}
