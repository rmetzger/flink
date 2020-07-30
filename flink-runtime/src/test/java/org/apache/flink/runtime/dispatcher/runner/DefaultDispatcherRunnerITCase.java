/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherBootstrap;
import org.apache.flink.runtime.dispatcher.DispatcherFactory;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.dispatcher.DispatcherServices;
import org.apache.flink.runtime.dispatcher.JobManagerRunnerFactory;
import org.apache.flink.runtime.dispatcher.MemoryArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServicesWithJobGraphStore;
import org.apache.flink.runtime.dispatcher.SessionDispatcherFactory;
import org.apache.flink.runtime.dispatcher.SingleJobJobGraphStore;
import org.apache.flink.runtime.dispatcher.StandaloneDispatcher;
import org.apache.flink.runtime.dispatcher.TestingJobManagerRunnerFactory;
import org.apache.flink.runtime.dispatcher.VoidHistoryServerArchivist;
import org.apache.flink.runtime.heartbeat.TestingHeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServicesBuilder;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.jobmaster.TestingJobManagerRunner;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcServiceResource;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.TestingJobGraphStore;
import org.apache.flink.runtime.util.BlobServerResource;
import org.apache.flink.runtime.util.LeaderConnectionInfo;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Integration tests for the {@link DefaultDispatcherRunner}.
 */
public class DefaultDispatcherRunnerITCase extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(DefaultDispatcherRunnerITCase.class);

	private static final Time TIMEOUT = Time.seconds(10L);

	private static final JobID TEST_JOB_ID = new JobID();

	@ClassRule
	public static TestingRpcServiceResource rpcServiceResource = new TestingRpcServiceResource();

	@ClassRule
	public static BlobServerResource blobServerResource = new BlobServerResource();

	private JobGraph jobGraph;

	private TestingLeaderElectionService dispatcherLeaderElectionService;

	private TestingFatalErrorHandler fatalErrorHandler;

	private JobGraphStore jobGraphStore;

	private PartialDispatcherServices partialDispatcherServices;

	private DefaultDispatcherRunnerFactory dispatcherRunnerFactory;

	@Before
	public void setup() {
		dispatcherRunnerFactory = DefaultDispatcherRunnerFactory.createSessionRunner(SessionDispatcherFactory.INSTANCE);
		jobGraph = createJobGraph();
		dispatcherLeaderElectionService = new TestingLeaderElectionService();
		fatalErrorHandler = new TestingFatalErrorHandler();
		jobGraphStore = TestingJobGraphStore.newBuilder().build();

		partialDispatcherServices = new PartialDispatcherServices(
			new Configuration(),
			new TestingHighAvailabilityServicesBuilder().build(),
			CompletableFuture::new,
			blobServerResource.getBlobServer(),
			new TestingHeartbeatServices(),
			UnregisteredMetricGroups::createUnregisteredJobManagerMetricGroup,
			new MemoryArchivedExecutionGraphStore(),
			fatalErrorHandler,
			VoidHistoryServerArchivist.INSTANCE,
			null);
	}

	@After
	public void teardown() throws Exception {
		if (fatalErrorHandler != null) {
			fatalErrorHandler.rethrowError();
		}
	}

	@Test(timeout = 5_000L)
	public void testNonBlockingJobSubmission() throws Exception {
		try (final DispatcherRunner dispatcherRunner = createDispatcherRunner()) {
			LOG.info("Starting test");
			final UUID firstLeaderSessionId = UUID.randomUUID();
			final DispatcherGateway dispatcherGateway = electLeaderAndRetrieveGateway(firstLeaderSessionId);

			// create a job graph of a job that blocks forever
			final BlockingJobVertex blockingJobVertex = new BlockingJobVertex("testVertex");
			blockingJobVertex.setInvokableClass(NoOpInvokable.class);
			JobGraph blockingJobGraph = new JobGraph(TEST_JOB_ID, "blockingTestJob", blockingJobVertex);

			CompletableFuture<Acknowledge> ackFuture = dispatcherGateway.submitJob(
				blockingJobGraph,
				TIMEOUT);

			// job submission needs to return within a reasonable timeframe
			LOG.info("Waiting on future");
			Assert.assertEquals(Acknowledge.get(), ackFuture.get(4, TimeUnit.SECONDS));

			LOG.info("Done waiting");

			// submission has succeeded, let the initialization finish.
			blockingJobVertex.unblock();

			LOG.info("checking job status:");
			// wait till job is running
			JobStatus status;
			do {
				status = dispatcherGateway.requestJobStatus(
					blockingJobGraph.getJobID(),
					TIMEOUT).get();
				Thread.sleep(50);
				LOG.info("Status = " + status);
			} while(status != JobStatus.RUNNING);
			LOG.info("job is running now");

			dispatcherGateway.cancelJob(blockingJobGraph.getJobID(), TIMEOUT).get();
			LOG.info("Job successfully cancelled");
		}
	}

	@Test(timeout = 5_000L)
	public void testCancellationDuringInitialization() throws Exception {
		try (final DispatcherRunner dispatcherRunner = createDispatcherRunner()) {
			LOG.info("Starting test");
			final UUID firstLeaderSessionId = UUID.randomUUID();
			final DispatcherGateway dispatcherGateway = electLeaderAndRetrieveGateway(firstLeaderSessionId);

			// create a job graph of a job that blocks forever
			final BlockingJobVertex blockingJobVertex = new BlockingJobVertex("testVertex");
			blockingJobVertex.setInvokableClass(NoOpInvokable.class);
			JobGraph blockingJobGraph = new JobGraph(TEST_JOB_ID, "blockingTestJob", blockingJobVertex);

			CompletableFuture<Acknowledge> ackFuture = dispatcherGateway.submitJob(
				blockingJobGraph,
				TIMEOUT);

			// job submission needs to return within a reasonable timeframe
			LOG.info("Waiting on future");
			Assert.assertEquals(Acknowledge.get(), ackFuture.get(4, TimeUnit.SECONDS));

			LOG.info("Done waiting");

			// submission has succeeded, now cancel the job
			dispatcherGateway.cancelJob(blockingJobGraph.getJobID(), TIMEOUT).get();
			LOG.info("Job successfully cancelled");
		}
	}


	@Test
	public void leaderChange_afterJobSubmission_recoversSubmittedJob() throws Exception {
		try (final DispatcherRunner dispatcherRunner = createDispatcherRunner()) {
			final UUID firstLeaderSessionId = UUID.randomUUID();

			final DispatcherGateway firstDispatcherGateway = electLeaderAndRetrieveGateway(firstLeaderSessionId);

			firstDispatcherGateway.submitJob(jobGraph, TIMEOUT).get();

			dispatcherLeaderElectionService.notLeader();

			final UUID secondLeaderSessionId = UUID.randomUUID();
			final DispatcherGateway secondDispatcherGateway = electLeaderAndRetrieveGateway(secondLeaderSessionId);

			final Collection<JobID> jobIds = secondDispatcherGateway.listJobs(TIMEOUT).get();

			assertThat(jobIds, contains(jobGraph.getJobID()));
		}
	}

	private DispatcherGateway electLeaderAndRetrieveGateway(UUID firstLeaderSessionId) throws InterruptedException, java.util.concurrent.ExecutionException {
		dispatcherLeaderElectionService.isLeader(firstLeaderSessionId);
		final LeaderConnectionInfo leaderConnectionInfo = dispatcherLeaderElectionService.getConfirmationFuture().get();

		return rpcServiceResource.getTestingRpcService().connect(
			leaderConnectionInfo.getAddress(),
			DispatcherId.fromUuid(leaderConnectionInfo.getLeaderSessionId()),
			DispatcherGateway.class).get();
	}

	/**
	 * See FLINK-11843. This is a probabilistic test which needs to be executed several times to fail.
	 */
	@Test
	public void leaderChange_withBlockingJobManagerTermination_doesNotAffectNewLeader() throws Exception {
		final TestingJobManagerRunnerFactory jobManagerRunnerFactory = new TestingJobManagerRunnerFactory(1);
		dispatcherRunnerFactory = DefaultDispatcherRunnerFactory.createSessionRunner(new TestingDispatcherFactory(jobManagerRunnerFactory));
		jobGraphStore = new SingleJobJobGraphStore(jobGraph);

		try (final DispatcherRunner dispatcherRunner = createDispatcherRunner()) {

			// initial run
			dispatcherLeaderElectionService.isLeader(UUID.randomUUID()).get();
			final TestingJobManagerRunner testingJobManagerRunner = jobManagerRunnerFactory.takeCreatedJobManagerRunner();

			dispatcherLeaderElectionService.notLeader();

			LOG.info("Re-grant leadership first time.");
			dispatcherLeaderElectionService.isLeader(UUID.randomUUID());

			// give the Dispatcher some time to recover jobs
			Thread.sleep(1L);

			dispatcherLeaderElectionService.notLeader();

			LOG.info("Re-grant leadership second time.");
			final UUID leaderSessionId = UUID.randomUUID();
			final CompletableFuture<UUID> leaderFuture = dispatcherLeaderElectionService.isLeader(leaderSessionId);
			assertThat(leaderFuture.isDone(), is(false));

			LOG.info("Complete the termination of the first job manager runner.");
			testingJobManagerRunner.completeTerminationFuture();

			assertThat(leaderFuture.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS), is(equalTo(leaderSessionId)));
		}
	}

	private static class TestingDispatcherFactory implements DispatcherFactory {
		private final JobManagerRunnerFactory jobManagerRunnerFactory;

		private TestingDispatcherFactory(JobManagerRunnerFactory jobManagerRunnerFactory) {
			this.jobManagerRunnerFactory = jobManagerRunnerFactory;
		}

		@Override
		public Dispatcher createDispatcher(
			RpcService rpcService,
			DispatcherId fencingToken,
			DispatcherBootstrap dispatcherBootstrap,
			PartialDispatcherServicesWithJobGraphStore partialDispatcherServicesWithJobGraphStore) throws Exception {
			return new StandaloneDispatcher(
				rpcService,
				fencingToken,
				dispatcherBootstrap,
				DispatcherServices.from(partialDispatcherServicesWithJobGraphStore, jobManagerRunnerFactory));
		}
	}

	private static JobGraph createJobGraph() {
		final JobVertex testVertex = new JobVertex("testVertex");
		testVertex.setInvokableClass(NoOpInvokable.class);
		return new JobGraph(TEST_JOB_ID, "testJob", testVertex);
	}

	private DispatcherRunner createDispatcherRunner() throws Exception {
		return dispatcherRunnerFactory.createDispatcherRunner(
			dispatcherLeaderElectionService,
			fatalErrorHandler,
			() -> jobGraphStore,
			TestingUtils.defaultExecutor(),
			rpcServiceResource.getTestingRpcService(),
			partialDispatcherServices);
	}

	private static class BlockingJobVertex extends JobVertex {
		private final Object lock = new Object();
		private boolean blocking = true;
		public BlockingJobVertex(String name) {
			super(name);
		}

		@Override
		public void initializeOnMaster(ClassLoader loader) throws Exception {
			super.initializeOnMaster(loader);

			while (true) {
				synchronized (lock) {
					if (!blocking) {
						LOG.info("Initialization is unblocked ...");
						return;
					}
					LOG.info("Initialization is waiting ... Lock on " + lock);
					lock.wait(10);
				}
			}
		}

		public void unblock() {
			LOG.info("Unblocking on " + lock);
			synchronized (lock) {
				blocking = false;
				LOG.info("Notifying");
				lock.notify();
			}
		}
	}
}
