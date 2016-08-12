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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.testutils.MockRuntimeContext;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.TestLogger;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Future;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

@RunWith(PowerMockRunner.class)
@PrepareForTest(FlinkKafkaProducerBase.class)
public class KafkaProducerTest extends TestLogger {
	
	@Test
	@SuppressWarnings("unchecked")
	public void testPropagateExceptions() {
		try {
			// mock kafka producer
			KafkaProducer<?, ?> kafkaProducerMock = mock(KafkaProducer.class);

			// partition setup
			when(kafkaProducerMock.partitionsFor(anyString())).thenReturn(
					Collections.singletonList(new PartitionInfo("mock_topic", 42, null, null, null)));

			// failure when trying to send an element
			when(kafkaProducerMock.send(any(ProducerRecord.class), any(Callback.class)))
				.thenAnswer(new Answer<Future<RecordMetadata>>() {
					@Override
					public Future<RecordMetadata> answer(InvocationOnMock invocation) throws Throwable {
						Callback callback = (Callback) invocation.getArguments()[1];
						callback.onCompletion(null, new Exception("Test error"));
						return null;
					}
				});

			// make sure the FlinkKafkaProducer instantiates our mock producer
			whenNew(KafkaProducer.class).withAnyArguments().thenReturn(kafkaProducerMock);

			// (1) producer that propagates errors


			FlinkKafkaProducer010<String> producerPropagating = newProducer();

			((RichFunction)producerPropagating.getUserFunction()).setRuntimeContext(new MockRuntimeContext(17, 3));
			producerPropagating.open();

			try {
				producerPropagating.processElement(new StreamRecord<>("value", 1L));
				producerPropagating.processElement(new StreamRecord<>("value", 1L));
				fail("This should fail with an exception");
			}
			catch (Exception e) {
				assertNotNull(e.getCause());
				assertNotNull(e.getCause().getMessage());
				assertTrue(e.getCause().getMessage().contains("Test error"));
			}
			// avoid reuse
			producerPropagating = null;

			// (2) producer that only logs errors

			FlinkKafkaProducer010<String> producerLogging = newProducer();
			((FlinkKafkaProducer09)producerLogging.getUserFunction()).setLogFailuresOnly(true);

			((RichFunction)producerLogging.getUserFunction()).setRuntimeContext(new MockRuntimeContext(17, 3));
			producerLogging.open();

			producerLogging.processElement(new StreamRecord<>("value", 1L));
			producerLogging.processElement(new StreamRecord<>("value", 1L));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private static FlinkKafkaProducer010 newProducer() {
		try {
			Class<FlinkKafkaProducer010> prodClass = FlinkKafkaProducer010.class;
			Constructor<FlinkKafkaProducer010> ctor = prodClass.getDeclaredConstructor(String.class, KeyedSerializationSchema.class, Properties.class, KafkaPartitioner.class);
			ctor.setAccessible(true);
			return ctor.newInstance("mock_topic", new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()), new Properties(), null);
		} catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
			throw new RuntimeException("Error while creating producer", e);
		}
	}
}
