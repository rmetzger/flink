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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.TestingJobManagerRunner;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static junit.framework.Assert.fail;
import static org.hamcrest.core.Is.is;

/**
 * Test for the {@link DispatcherJob} class.
 */
public class DispatcherJobTest extends TestLogger {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	private static final Time TIMEOUT = Time.seconds(10L);
	private static final JobID TEST_JOB_ID = new JobID();

	@Test
	public void testStatusWhenInitializing() throws
		Exception {
		TestContext testContext = createTestContext(Dispatcher.ExecutionType.SUBMISSION);
		DispatcherJob dispatcherJob = testContext.getDispatcherJob();

		Assert.assertThat(dispatcherJob.isInitialized(), is(false));
		Assert.assertThat(dispatcherJob.getResultFuture().isDone(), is(false));
		assertJobStatus(dispatcherJob, JobStatus.INITIALIZING);
	}

	@Test
	public void testStatusWhenRunning() throws
		Exception {
		TestContext testContext = createTestContext(Dispatcher.ExecutionType.SUBMISSION);
		DispatcherJob dispatcherJob = testContext.getDispatcherJob();

		// finish initialization
		testContext.setRunning();

		assertJobStatus(dispatcherJob, JobStatus.RUNNING);

		// result future not done
		Assert.assertThat(dispatcherJob.getResultFuture().isDone(), is(false));

		Assert.assertThat(dispatcherJob.isInitialized(), is(true));
	}

	@Test
	public void testStatusWhenJobFinished() throws
		Exception {
		TestContext testContext = createTestContext(Dispatcher.ExecutionType.SUBMISSION);
		DispatcherJob dispatcherJob = testContext.getDispatcherJob();

		// finish job
		testContext.setRunning();
		testContext.finishJob();

		assertJobStatus(dispatcherJob, JobStatus.FINISHED);

		// assert result future done
		ArchivedExecutionGraph aeg = dispatcherJob.getResultFuture().get();

		Assert.assertThat(aeg.getState(), is(JobStatus.FINISHED));
	}

	@Test
	public void testStatusWhenCancellingWhileInitializing() throws
		Exception {
		TestContext testContext = createTestContext(Dispatcher.ExecutionType.SUBMISSION);
		DispatcherJob dispatcherJob = testContext.getDispatcherJob();
		assertJobStatus(dispatcherJob, JobStatus.INITIALIZING);

		CompletableFuture<Acknowledge> cancelFuture = dispatcherJob.cancel(
			TIMEOUT);

		Assert.assertThat(cancelFuture.isDone(), is(false));
		Assert.assertThat(dispatcherJob.isInitialized(), is(false));

		assertJobStatus(dispatcherJob, JobStatus.CANCELLING);

		testContext.setRunning();
		testContext.finishCancellation();

		// assert that cancel future completes
		cancelFuture.get();

		assertJobStatus(dispatcherJob, JobStatus.CANCELED);
		Assert.assertThat(dispatcherJob.isInitialized(), is(true));
		// assert that the result future completes
		Assert.assertThat(dispatcherJob.getResultFuture().get().getState(), is(JobStatus.CANCELED));
	}

	@Test
	public void testStatusWhenCancellingWhileRunning() throws
		Exception {
		TestContext testContext = createTestContext(Dispatcher.ExecutionType.SUBMISSION);
		DispatcherJob dispatcherJob = testContext.getDispatcherJob();

		testContext.setRunning();
		CompletableFuture<Acknowledge> cancelFuture = dispatcherJob.cancel(TIMEOUT);

		assertJobStatus(dispatcherJob, JobStatus.CANCELLING);
		testContext.finishCancellation();

		cancelFuture.get();
		assertJobStatus(dispatcherJob, JobStatus.CANCELED);
		Assert.assertThat(dispatcherJob.getResultFuture().get().getState(), is(JobStatus.CANCELED));
	}

	@Test
	public void testStatusWhenCancellingWhileFailed() throws
		Exception {
		TestContext testContext = createTestContext(Dispatcher.ExecutionType.SUBMISSION);
		DispatcherJob dispatcherJob = testContext.getDispatcherJob();

		RuntimeException exception = new RuntimeException("Artificial failure in runner initialization");
		testContext.failInitialization(exception);

		assertJobStatus(dispatcherJob, JobStatus.FAILED);

		CommonTestUtils.assertThrows("Artificial failure", ExecutionException.class, () -> dispatcherJob.cancel(TIMEOUT).get());

		assertJobStatus(dispatcherJob, JobStatus.FAILED);
	}

	@Test
	public void testErrorWhileInitializing() throws Exception {
		TestContext testContext = createTestContext(Dispatcher.ExecutionType.SUBMISSION);
		DispatcherJob dispatcherJob = testContext.dispatcherJob;

		// now fail
		RuntimeException exception = new RuntimeException("Artificial failure in runner initialization");
		testContext.failInitialization(exception);

		Assert.assertThat(dispatcherJob.isInitialized(), is(true));
		assertJobStatus(dispatcherJob, JobStatus.FAILED);

		ArchivedExecutionGraph aeg = dispatcherJob.getResultFuture().get();
		Assert.assertThat(aeg.getFailureInfo().getException().deserializeError(ClassLoader.getSystemClassLoader()), is(exception));

		Assert.assertTrue(dispatcherJob.closeAsync().isDone() && dispatcherJob.closeAsync().isCompletedExceptionally());
	}

	@Test
	public void testCloseWhileInitializingSuccessfully() throws Exception {
		TestContext testContext = createTestContext(Dispatcher.ExecutionType.SUBMISSION);
		DispatcherJob dispatcherJob = testContext.getDispatcherJob();

		CompletableFuture<Void> closeFuture = dispatcherJob.closeAsync();
		Assert.assertThat(closeFuture.isDone(), is(false));

		// set job running, so that we can cancel it
		testContext.setRunning();

		// assert future completes now
		closeFuture.get();

		// ensure the result future is complete (how it completes is up to the JobManager)
		CompletableFuture<ArchivedExecutionGraph> resultFuture = dispatcherJob.getResultFuture();
		CommonTestUtils.assertThrows("has not been finished", ExecutionException.class,
			resultFuture::get);
	}

	@Test
	public void testCloseWhileInitializingErroneously() throws Exception {
		TestContext testContext = createTestContext(Dispatcher.ExecutionType.SUBMISSION);
		DispatcherJob dispatcherJob = testContext.getDispatcherJob();

		CompletableFuture<Void> closeFuture = dispatcherJob.closeAsync();
		Assert.assertThat(closeFuture.isDone(), is(false));

		testContext.failInitialization(new RuntimeException("fail"));

		// assert future completes now
		CommonTestUtils.assertThrows("fail", ExecutionException.class,
			closeFuture::get);

		// ensure the result future is complete
		Assert.assertThat(dispatcherJob.getResultFuture().get().getState(), is(JobStatus.FAILED));
	}

	@Test
	public void testCloseWhileInitializingErroneouslyForRecovery() {
		TestContext testContext = createTestContext(Dispatcher.ExecutionType.RECOVERY);
		DispatcherJob dispatcherJob = testContext.getDispatcherJob();

		CompletableFuture<Void> closeFuture = dispatcherJob.closeAsync();

		testContext.failInitialization(new RuntimeException("fail"));

		CommonTestUtils.assertThrows("fail", ExecutionException.class,
			closeFuture::get);
		// ensure the result future is completing exceptionally when using RECOVERY execution
		CommonTestUtils.assertThrows("fail", ExecutionException.class,
			() -> dispatcherJob.getResultFuture().get());
	}

	@Test
	public void testCloseWhileRunning() throws Exception {
		TestContext testContext = createTestContext(Dispatcher.ExecutionType.SUBMISSION);
		DispatcherJob dispatcherJob = testContext.getDispatcherJob();

		// complete JobManager runner future to indicate to the DispatcherJob that the Runner has been initialized
		testContext.setRunning();

		CompletableFuture<Void> closeFuture = dispatcherJob.closeAsync();

		closeFuture.get();

		// result future should complete exceptionally.
		CompletableFuture<ArchivedExecutionGraph> resultFuture = dispatcherJob.getResultFuture();
		CommonTestUtils.assertThrows("has not been finished", ExecutionException.class,
			resultFuture::get);
	}

	private TestContext createTestContext(Dispatcher.ExecutionType type) {
		final JobVertex testVertex = new JobVertex("testVertex");
		testVertex.setInvokableClass(NoOpInvokable.class);

		JobGraph jobGraph = new JobGraph(TEST_JOB_ID, "testJob", testVertex);
		CompletableFuture<JobManagerRunner> jobManagerRunnerCompletableFuture = new CompletableFuture<>();
		DispatcherJob dispatcherJob = DispatcherJob.createFor(jobManagerRunnerCompletableFuture,
			jobGraph.getJobID(), jobGraph.getName(), type);

		return new TestContext(
			jobManagerRunnerCompletableFuture,
			dispatcherJob,
			jobGraph);
	}

	private static class TestContext {
		private final CompletableFuture<JobManagerRunner> jobManagerRunnerCompletableFuture;
		private final DispatcherJob dispatcherJob;
		private final JobGraph jobGraph;
		private final TestingJobMasterGateway mockRunningJobMasterGateway;
		private final CompletableFuture<Acknowledge> cancellationFuture;

		private JobStatus internalJobStatus = JobStatus.INITIALIZING;

		public TestContext(
			CompletableFuture<JobManagerRunner> jobManagerRunnerCompletableFuture,
			DispatcherJob dispatcherJob,
			JobGraph jobGraph) {
			this.jobManagerRunnerCompletableFuture = jobManagerRunnerCompletableFuture;
			this.dispatcherJob = dispatcherJob;
			this.jobGraph = jobGraph;

			this.cancellationFuture = new CompletableFuture<>();
			this.mockRunningJobMasterGateway = new TestingJobMasterGatewayBuilder()
				.setRequestJobSupplier(() -> CompletableFuture.completedFuture(ArchivedExecutionGraph.createFromInitializingJob(getJobID(), "test", internalJobStatus, null, 1337)))
				.setRequestJobDetailsSupplier(() -> {
					JobDetails jobDetails = new JobDetails(getJobID(), "", 0, 0, 0, internalJobStatus, 0,
						new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0}, 0);
					return CompletableFuture.completedFuture(jobDetails);
				})
				// once JobManagerRunner is initialized, complete result future with CANCELLED AEG and ack cancellation.
				.setCancelFunction(() -> {
					internalJobStatus = JobStatus.CANCELLING;
					return cancellationFuture;
				})
				.build();
		}

		public JobID getJobID() {
			return jobGraph.getJobID();
		}

		public void failInitialization(Throwable ex) {
			jobManagerRunnerCompletableFuture.completeExceptionally(ex);
		}

		public DispatcherJob getDispatcherJob() {
			return dispatcherJob;
		}

		public void setRunning() {
			internalJobStatus = JobStatus.RUNNING;
			TestingJobManagerRunner jobManagerRunner =
				new TestingJobManagerRunner(getJobID(), false);
			jobManagerRunner.getJobMasterGateway().complete(mockRunningJobMasterGateway);
			jobManagerRunnerCompletableFuture.complete(jobManagerRunner);
		}

		public void finishJob() {
			try {
				internalJobStatus = JobStatus.FINISHED;
				jobManagerRunnerCompletableFuture.get().getResultFuture()
					.complete(ArchivedExecutionGraph.createFromInitializingJob(getJobID(), "test", JobStatus.FINISHED, null, 1337));
			} catch (Throwable e) {
				fail("Error in test infrastructure");
			}
		}

		public void finishCancellation() {
			jobManagerRunnerCompletableFuture.thenAccept(runner -> {
				internalJobStatus = JobStatus.CANCELED;
				runner.getResultFuture()
					.complete(ArchivedExecutionGraph.createFromInitializingJob(getJobID(), "test", JobStatus.CANCELED, null, 1337));
				cancellationFuture.complete(Acknowledge.get());
			});
		}
	}

	private void assertJobStatus(DispatcherJob dispatcherJob, JobStatus expectedStatus) throws
		Exception {
		Assert.assertThat(dispatcherJob.requestJobDetails(TIMEOUT).get().getStatus(), is(expectedStatus));
		Assert.assertThat(dispatcherJob.requestJob(TIMEOUT).get().getState(), is(expectedStatus));
		Assert.assertThat(dispatcherJob.requestJobStatus(TIMEOUT).get(), is(expectedStatus));
	}
}
