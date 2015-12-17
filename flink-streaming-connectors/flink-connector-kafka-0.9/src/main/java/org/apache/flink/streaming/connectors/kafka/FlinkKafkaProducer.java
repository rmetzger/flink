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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import org.apache.kafka.clients.producer.ProducerRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


/**
 * Flink Sink to produce data into a Kafka topic.
 *
 * Please note that this producer does not have any reliability guarantees.
 *
 * @param <IN> Type of the messages to write into Kafka.
 */
public class FlinkKafkaProducer<IN> extends FlinkKafkaProducerBase<IN> {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaProducerBase.class);

	private static final long serialVersionUID = 1L;

	// ------------------- Keyless serialization schema constructors ----------------------
	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces its input to
	 * the topic.
	 *
	 * @param brokerList
	 *			Comma separated addresses of the brokers
	 * @param topicId
	 * 			ID of the Kafka topic.
	 * @param serializationSchema
	 * 			User defined (keyless) serialization schema.
	 */
	public FlinkKafkaProducer(String brokerList, String topicId, SerializationSchema<IN> serializationSchema) {
		this(topicId, new KeyedSerializationSchemaWrapper<>(serializationSchema), getPropertiesFromBrokerList(brokerList), null);
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces its input to
	 * the topic.
	 *
	 * @param topicId
	 * 			ID of the Kafka topic.
	 * @param serializationSchema
	 * 			User defined (keyless) serialization schema.
	 * @param producerConfig
	 * 			Properties with the producer configuration.
	 */
	public FlinkKafkaProducer(String topicId, SerializationSchema<IN> serializationSchema, Properties producerConfig) {
		this(topicId, new KeyedSerializationSchemaWrapper<>(serializationSchema), producerConfig, null);
	}

	/**
	 * The main constructor for creating a FlinkKafkaProducer.
	 *
	 * @param topicId The topic to write data to
	 * @param serializationSchema A (keyless) serializable serialization schema for turning user objects into a kafka-consumable byte[]
	 * @param producerConfig Configuration properties for the KafkaProducer. 'bootstrap.servers.' is the only required argument.
	 * @param customPartitioner A serializable partitioner for assining messages to Kafka partitions.
	 */
	public FlinkKafkaProducer(String topicId, SerializationSchema<IN> serializationSchema, Properties producerConfig, KafkaPartitioner customPartitioner) {
		this(topicId, new KeyedSerializationSchemaWrapper<>(serializationSchema), producerConfig, customPartitioner);

	}

	// ------------------- Key/Value serialization schema constructors ----------------------

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces its input to
	 * the topic.
	 *
	 * @param brokerList
	 *			Comma separated addresses of the brokers
	 * @param topicId
	 * 			ID of the Kafka topic.
	 * @param serializationSchema
	 * 			User defined serialization schema supporting key/value messages
	 */
	public FlinkKafkaProducer(String brokerList, String topicId, KeyedSerializationSchema<IN> serializationSchema) {
		this(topicId, serializationSchema, getPropertiesFromBrokerList(brokerList), null);
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces its input to
	 * the topic.
	 *
	 * @param topicId
	 * 			ID of the Kafka topic.
	 * @param serializationSchema
	 * 			User defined serialization schema supporting key/value messages
	 * @param producerConfig
	 * 			Properties with the producer configuration.
	 */
	public FlinkKafkaProducer(String topicId, KeyedSerializationSchema<IN> serializationSchema, Properties producerConfig) {
		this(topicId, serializationSchema, producerConfig, null);
	}

	/**
	 * The main constructor for creating a FlinkKafkaProducer.
	 *
	 * @param topicId The topic to write data to
	 * @param serializationSchema A serializable serialization schema for turning user objects into a kafka-consumable byte[] supporting key/value messages
	 * @param producerConfig Configuration properties for the KafkaProducer. 'bootstrap.servers.' is the only required argument.
	 * @param customPartitioner A serializable partitioner for assining messages to Kafka partitions.
	 */
	public FlinkKafkaProducer(String topicId, KeyedSerializationSchema<IN> serializationSchema, Properties producerConfig, KafkaPartitioner customPartitioner) {
		super(topicId, serializationSchema, producerConfig, customPartitioner);
	}

	/**
	 * Called when new data arrives to the sink, and forwards it to Kafka.
	 *
	 * @param next
	 * 		The incoming data
	 */
	@Override
	public void invoke(IN next) throws Exception {
		// propagate asynchronous errors
		checkErroneous();

		byte[] serializedKey = schema.serializeKey(next);
		byte[] serializedValue = schema.serializeValue(next);
		ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topicId,
				/*partitioner.partition(next, partitions.length), */
				serializedKey, serializedValue);
		
		producer.send(record, callback);
	}

}
