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

package org.apache.flink.streaming.connectors.kafka.api;

import java.util.Properties;

import com.google.common.base.Preconditions;
import kafka.serializer.StringEncoder;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.streaming.api.function.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.api.config.PartitionerWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.SerializableKafkaPartitioner;
import org.apache.flink.streaming.connectors.util.SerializationSchema;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.DefaultEncoder;
import org.apache.flink.util.NetUtils;

/**
 * Sink that emits its inputs to a Kafka topic.
 *
 * @param <IN>
 * 		Type of the sink input
 */
public class KafkaSink<IN> extends RichSinkFunction<IN> {
	private static final long serialVersionUID = 1L;
	private Producer<IN, byte[]> producer;
	private Properties props;
	private String topicId;
	private String brokerAddr;
	private boolean initDone = false;
	private SerializationSchema<IN, byte[]> scheme;
	private SerializableKafkaPartitioner partitioner;
	private Class<? extends SerializableKafkaPartitioner> partitionerClass = null;

	/**
	 * Creates a KafkaSink for a given topic. The partitioner distributes the
	 * messages between the partitions of the topics.
	 *
	 * @param brokerAddr
	 * 		Address of the Kafka broker (with port number).
	 * @param topicId
	 * 		ID of the Kafka topic.
	 * @param serializationSchema
	 * 		User defined serialization schema.
	 */
	public KafkaSink(String brokerAddr, String topicId,
			SerializationSchema<IN, byte[]> serializationSchema) {
		this(brokerAddr, topicId, serializationSchema, (Class)null);
	}

	/**
	 * Creates a KafkaSink for a given topic. The sink produces its input into
	 * the topic.
	 *
	 * @param brokerAddr
	 * 		Address of the Kafka broker (with port number).
	 * @param topicId
	 * 		ID of the Kafka topic.
	 * @param serializationSchema
	 * 		User defined serialization schema.
	 * @param partitioner
	 * 		User defined partitioner.
	 */
	public KafkaSink(String brokerAddr, String topicId,
			SerializationSchema<IN, byte[]> serializationSchema, SerializableKafkaPartitioner partitioner) {
		NetUtils.ensureCorrectHostnamePort(brokerAddr);
		Preconditions.checkNotNull(topicId, "TopicID not set");
		ClosureCleaner.ensureSerializable(partitioner);

		this.topicId = topicId;
		this.brokerAddr = brokerAddr;
		this.scheme = serializationSchema;
		this.partitioner = partitioner;
	}

	public KafkaSink(String brokerAddr, String topicId,
					SerializationSchema<IN, byte[]> serializationSchema, Class<? extends SerializableKafkaPartitioner> partitioner) {
		NetUtils.ensureCorrectHostnamePort(brokerAddr);
		Preconditions.checkNotNull(topicId, "TopicID not set");
		ClosureCleaner.ensureSerializable(partitioner);

		this.topicId = topicId;
		this.brokerAddr = brokerAddr;
		this.scheme = serializationSchema;
		this.partitionerClass = partitioner;
	}

	/**
	 * Initializes the connection to Kafka.
	 */
	public void initialize() {

		props = new Properties();

		props.put("metadata.broker.list", brokerAddr);
		props.put("request.required.acks", "1");

		props.put("serializer.class", DefaultEncoder.class.getCanonicalName());
		props.put("key.serializer.class", StringEncoder.class.getCanonicalName());

		if(partitioner != null) {
			props.put("partitioner.class", PartitionerWrapper.class.getCanonicalName());
			// java serialization will do the rest.
			props.put(PartitionerWrapper.SERIALIZED_WRAPPER_NAME, partitioner);
		}
		if(partitionerClass != null) {
			props.put("partitioner.class", partitionerClass);
		}

		ProducerConfig config = new ProducerConfig(props);

		try {
			producer = new Producer<IN, byte[]>(config);
		} catch (NullPointerException e) {
			throw new RuntimeException("Cannot connect to Kafka broker " + brokerAddr);
		}
		initDone = true;
	}

	/**
	 * Called when new data arrives to the sink, and forwards it to Kafka.
	 *
	 * @param next
	 * 		The incoming data
	 */
	@Override
	public void invoke(IN next) {
		if (!initDone) {
			initialize();
		}

		byte[] serialized = scheme.serialize(next);
		producer.send(new KeyedMessage<IN, byte[]>(topicId, next, serialized));
	}

	@Override
	public void close() {
		if (producer != null) {
			producer.close();
		}
	}

	@Override
	public void cancel() {
		close();
	}

}
