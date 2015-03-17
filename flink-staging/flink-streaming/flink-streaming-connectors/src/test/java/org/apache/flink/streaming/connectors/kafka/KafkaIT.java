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

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSink;
import org.apache.flink.streaming.connectors.kafka.api.simple.KafkaTopicUtils;
import org.apache.flink.streaming.connectors.kafka.api.simple.PersistentKafkaSource;
import org.apache.flink.streaming.connectors.util.JavaDefaultStringSchema;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.config.PropertyParser;
import com.github.sakserv.minicluster.impl.KafkaLocalBroker;
import com.github.sakserv.minicluster.impl.ZookeeperLocalCluster;

public class KafkaIT {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaIT.class);

	private final String TOPIC = "myTopic";
	private final int CONSUMER_PARALLELISM = 1;


	private String zookeeperConnectionString;
	private String kafkaBrokerHost;

	@Test
	public void test() {
		ZookeeperLocalCluster zookeeper = zookeeper();
		KafkaLocalBroker broker1 = kafkaBroker(0);

		zookeeperConnectionString = zookeeper.getZookeeperConnectionString();
		kafkaBrokerHost = broker1.getKafkaHostname() + ":" + broker1.getKafkaPort();

		zookeeper.start();
		broker1.start();

		createTestTopic();

		startKafkaTopology();

		broker1.stop();
		zookeeper.stop();

		try {
			FileUtils.deleteDirectory(new File(zookeeper.getTempDir()));
			FileUtils.deleteDirectory(new File(broker1.getKafkaTempDir()));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void createTestTopic() {
		KafkaTopicUtils kafkaTopicUtils = new KafkaTopicUtils(zookeeperConnectionString);
		kafkaTopicUtils.createTopic(TOPIC, 1, 1);
	}

	private void startKafkaTopology() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		// TODO use a consumer and a producer
		DataStream<String> consumer = env.addSource(new PersistentKafkaSource<String>(zookeeperConnectionString, TOPIC, new JavaDefaultStringSchema()))
				.setParallelism(CONSUMER_PARALLELISM);
		consumer.print();

		env.fromElements(1).connect(consumer)
				.flatMap(new RichCoFlatMapFunction<Integer, String, String>() {
					boolean gotMessage;

					@Override
					public void open(Configuration configuration) {
						gotMessage = false;
					}

					@Override
					public void flatMap1(Integer s, Collector<String> collector) throws Exception {
						// wait for consumer
						while (!gotMessage) {
							collector.collect("msg");
							Thread.sleep(10);
						}

						// if done, close consumer using schemas end of stream checking
						for (int i = 0; i < CONSUMER_PARALLELISM; i++) {
							collector.collect("q");
						}
					}

					@Override
					public void flatMap2(String s, Collector<String> collector) throws Exception {
						// if this is received, the consumer started processing messages
						gotMessage = true;
					}

				})
				.addSink(new KafkaSink<String>(kafkaBrokerHost, TOPIC, new JavaDefaultStringSchema()));

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	/**
	 * Copied from com.github.sakserv.minicluster.KafkaLocalBrokerIntegrationTest (ASL licensed)
	 */
	// Setup the property parser
	private static PropertyParser propertyParser;

	static {
		try {
			propertyParser = new PropertyParser(ConfigVars.DEFAULT_PROPS_FILE);
		} catch (IOException e) {
			LOG.error("Unable to load property file: " + propertyParser.getProperty(ConfigVars.DEFAULT_PROPS_FILE));
		}
	}

	/**
	 * Copied from com.github.sakserv.minicluster.KafkaLocalBrokerIntegrationTest (ASL licensed)
	 */
	private ZookeeperLocalCluster zookeeper() {
		ZookeeperLocalCluster zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
				.setPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.ZOOKEEPER_PORT_KEY)))
				.setTempDir(propertyParser.getProperty(ConfigVars.ZOOKEEPER_TEMP_DIR_KEY))
				.setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
				.build();
		return zookeeperLocalCluster;
	}

	/**
	 * Copied from com.github.sakserv.minicluster.KafkaLocalBrokerIntegrationTest (ASL licensed)
	 */
	private KafkaLocalBroker kafkaBroker(int brokerId) {
		KafkaLocalBroker kafkaLocalBroker = new KafkaLocalBroker.Builder()
				.setKafkaHostname(propertyParser.getProperty(ConfigVars.KAFKA_HOSTNAME_KEY))
				.setKafkaPort(Integer.parseInt(propertyParser.getProperty(ConfigVars.KAFKA_PORT_KEY)))
				.setKafkaBrokerId(brokerId)
				.setKafkaProperties(new Properties())
				.setKafkaTempDir(propertyParser.getProperty(ConfigVars.KAFKA_TEST_TEMP_DIR_KEY))
				.setZookeeperConnectionString(propertyParser.getProperty(ConfigVars.ZOOKEEPER_CONNECTION_STRING_KEY))
				.build();
		return kafkaLocalBroker;
	}

}
