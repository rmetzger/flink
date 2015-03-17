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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;
import org.apache.curator.test.TestingServer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSink;
import org.apache.flink.streaming.connectors.kafka.api.simple.KafkaTopicUtils;
import org.apache.flink.streaming.connectors.kafka.api.simple.PersistentKafkaSource;
import org.apache.flink.streaming.connectors.util.JavaDefaultStringSchema;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Code in this test is based on the following GitHub repository:
 * (as per commit bc6b2b2d5f6424d5f377aa6c0871e82a956462ef)
 *
 * https://github.com/sakserv/hadoop-mini-clusters (ASL licensed)
 */

public class KafkaITCase {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaITCase.class);

	private final String TOPIC = "myTopic";
	private final int CONSUMER_PARALLELISM = 1;

	private final int ZK_PORT = 6667;
	private final int KAFKA_PORT = 6668;
	private String kafkaHost;
	private String zookeeperConnectionString;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	public File tmpZkDir;
	public File tmpKafkaDir;
	@Before
	public void prepare() throws IOException {
		tmpZkDir = tempFolder.newFolder();
		tmpKafkaDir = tempFolder.newFolder();
		kafkaHost = InetAddress.getLocalHost().getHostName();
		zookeeperConnectionString = "localhost:"+ZK_PORT;
	}


	@Test
	public void test() {
		LOG.info("Starting KafkaITCase.test()");
		TestingServer zookeeper = null;
		KafkaServer broker1 = null;
		try {
			zookeeper = getZookeper();
			broker1 = getKafkaServer(0);
			LOG.info("ZK and KafkaServer started. Creating test topic:");
			createTestTopic();

			LOG.info("Starting Kafka Topology in Flink:");
			startKafkaTopology();

			LOG.info("Test suceeded. Shutting down services");

		} catch(Exception t) {
			LOG.warn("Test failed with exception", t);
			Assert.fail("Test failed with: " + t.getMessage());
		} finally {
			if (zookeeper != null) {
				try {
					zookeeper.stop();
				} catch (IOException e) {
					LOG.warn("ZK.stop() failed",e);
				}
			}
			if (broker1 != null) {
				broker1.shutdown();
			}

		}

	}

	private void createTestTopic() {
		KafkaTopicUtils kafkaTopicUtils = new KafkaTopicUtils(zookeeperConnectionString);
		kafkaTopicUtils.createTopic(TOPIC, 1, 1);
	}

	private void startKafkaTopology() throws Exception {
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
				.addSink(new KafkaSink<String>(kafkaHost+":"+KAFKA_PORT, TOPIC, new JavaDefaultStringSchema()));

		env.execute();
	}


	private TestingServer getZookeper() throws Exception {
		return new TestingServer(ZK_PORT, tmpZkDir);
	}

	/**
	 * Copied from com.github.sakserv.minicluster.KafkaLocalBrokerIntegrationTest (ASL licensed)
	 */
	private KafkaServer getKafkaServer(int brokerId) throws UnknownHostException {
		Properties kafkaProperties = new Properties();
		// properties have to be Strings
		kafkaProperties.put("advertised.host.name", kafkaHost);
		kafkaProperties.put("port", Integer.toString(KAFKA_PORT));
		kafkaProperties.put("broker.id", Integer.toString(brokerId));
		kafkaProperties.put("log.dir", tmpKafkaDir.toString());
		kafkaProperties.put("zookeeper.connect", zookeeperConnectionString);
		KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);

		KafkaServer server = new KafkaServer(kafkaConfig, new LocalSystemTime());
		server.startup();
		return server;
	}

	public class LocalSystemTime implements Time {

		@Override
		public long milliseconds() {
			return System.currentTimeMillis();
		}

		public long nanoseconds() {
			return System.nanoTime();
		}

		@Override
		public void sleep(long ms) {
			try {
				Thread.sleep(ms);
			} catch (InterruptedException e) {
				LOG.warn("Interruption", e);
			}
		}

	}

}
