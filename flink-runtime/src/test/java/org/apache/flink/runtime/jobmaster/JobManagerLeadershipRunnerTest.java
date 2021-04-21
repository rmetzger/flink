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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.TestingClassLoaderLease;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.factories.TestingJobMasterServiceProcessFactory;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.runtime.util.TestingUserCodeClassLoader;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** Tests for the {@link JobManagerLeadershipRunner}. */
public class JobManagerLeadershipRunnerTest extends TestLogger {

    private static final Time TESTING_TIMEOUT = Time.seconds(10);

    @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static JobGraph jobGraph;

    private static ExecutionGraphInfo executionGraphInfo;

    private static JobMasterServiceProcessFactory defaultJobMasterServiceProcessFactory;

    private TestingHighAvailabilityServices haServices;

    private TestingLeaderElectionService leaderElectionService;

    private TestingFatalErrorHandler fatalErrorHandler;

    @BeforeClass
    public static void setupClass() {
        defaultJobMasterServiceProcessFactory = new TestingJobMasterServiceProcessFactory();

        final JobVertex jobVertex = new JobVertex("Test vertex");
        jobVertex.setInvokableClass(NoOpInvokable.class);
        jobGraph = JobGraphTestUtils.streamingJobGraph(jobVertex);

        executionGraphInfo =
                new ExecutionGraphInfo(
                        new ArchivedExecutionGraphBuilder()
                                .setJobID(jobGraph.getJobID())
                                .setState(JobStatus.FINISHED)
                                .build());
    }

    @Before
    public void setup() {
        leaderElectionService = new TestingLeaderElectionService();
        haServices = new TestingHighAvailabilityServices();
        haServices.setJobMasterLeaderElectionService(jobGraph.getJobID(), leaderElectionService);
        haServices.setResourceManagerLeaderRetriever(new SettableLeaderRetrievalService());
        haServices.setCheckpointRecoveryFactory(new StandaloneCheckpointRecoveryFactory());

        fatalErrorHandler = new TestingFatalErrorHandler();
    }

    @After
    public void tearDown() throws Exception {
        fatalErrorHandler.rethrowError();
    }

    @Test
    public void testJobCompletion() throws Exception {
        final JobManagerLeadershipRunner jobManagerRunner = createJobManagerLeadershipRunner();

        try {
            jobManagerRunner.start();

            final CompletableFuture<JobManagerRunnerResult> resultFuture =
                    jobManagerRunner.getResultFuture();

            assertThat(resultFuture.isDone(), is(false));

            jobManagerRunner.jobReachedGloballyTerminalState(executionGraphInfo);

            final JobManagerRunnerResult jobManagerRunnerResult = resultFuture.get();
            assertThat(
                    jobManagerRunnerResult,
                    is(JobManagerRunnerResult.forSuccess(executionGraphInfo)));
        } finally {
            jobManagerRunner.close();
        }
    }

    @Test
    public void testJobFinishedByOther() throws Exception {
        final JobManagerLeadershipRunner jobManagerRunner = createJobManagerLeadershipRunner();

        try {
            jobManagerRunner.start();

            final CompletableFuture<JobManagerRunnerResult> resultFuture =
                    jobManagerRunner.getResultFuture();

            assertThat(resultFuture.isDone(), is(false));

            jobManagerRunner.jobFinishedByOther();

            assertJobNotFinished(resultFuture);
        } finally {
            jobManagerRunner.close();
        }
    }

    @Test
    public void testShutDown() throws Exception {
        final JobManagerRunner jobManagerRunner = createJobManagerLeadershipRunner();

        try {
            jobManagerRunner.start();

            final CompletableFuture<JobManagerRunnerResult> resultFuture =
                    jobManagerRunner.getResultFuture();

            assertThat(resultFuture.isDone(), is(false));

            jobManagerRunner.closeAsync();

            assertJobNotFinished(resultFuture);
        } finally {
            jobManagerRunner.close();
        }
    }

    @Test
    public void testLibraryCacheManagerRegistration() throws Exception {
        final OneShotLatch registerClassLoaderLatch = new OneShotLatch();
        final OneShotLatch closeClassLoaderLeaseLatch = new OneShotLatch();
        final TestingUserCodeClassLoader userCodeClassLoader =
                TestingUserCodeClassLoader.newBuilder().build();
        final TestingClassLoaderLease classLoaderLease =
                TestingClassLoaderLease.newBuilder()
                        .setGetOrResolveClassLoaderFunction(
                                (permanentBlobKeys, urls) -> {
                                    registerClassLoaderLatch.trigger();
                                    return userCodeClassLoader;
                                })
                        .setCloseRunnable(closeClassLoaderLeaseLatch::trigger)
                        .build();
        final JobManagerRunner jobManagerRunner =
                createJobManagerLeadershipRunner(classLoaderLease);

        try {
            jobManagerRunner.start();

            registerClassLoaderLatch.await();

            jobManagerRunner.close();

            closeClassLoaderLeaseLatch.await();
        } finally {
            jobManagerRunner.close();
        }
    }

    /**
     * Tests that the {@link JobManagerRunnerImpl} always waits for the previous leadership
     * operation (granting or revoking leadership) to finish before starting a new leadership
     * operation.
     */
    /*   @Test
    public void testConcurrentLeadershipOperationsBlockingClose() throws Exception {
        final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();

        TestingJobMasterServiceFactory jobMasterServiceFactory =
                new TestingJobMasterServiceFactory(
                        () -> new TestingJobMasterService("localhost", terminationFuture, null));
        JobManagerRunner jobManagerRunner =
                createJobManagerLeadershipRunner(jobMasterServiceFactory);

        jobManagerRunner.start();

        leaderElectionService.isLeader(UUID.randomUUID()).get();

        leaderElectionService.notLeader();

        final CompletableFuture<UUID> leaderFuture =
                leaderElectionService.isLeader(UUID.randomUUID());

        // the new leadership should wait first for the suspension to happen
        assertThat(leaderFuture.isDone(), is(false));

        try {
            leaderFuture.get(1L, TimeUnit.MILLISECONDS);
            fail("Granted leadership even though the JobMaster has not been suspended.");
        } catch (TimeoutException expected) {
            // expected
        }

        terminationFuture.complete(null);

        leaderFuture.get();
    } */

    @Test
    public void testJobMasterServiceTerminatesUnexpectedlyTriggersFailure() throws Exception {
        final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();

        TestingJobMasterServiceProcessFactory jobMasterServiceProcessFactory =
                new TestingJobMasterServiceProcessFactory(
                        CompletableFuture.completedFuture(
                                new TestingJobMasterService("localhost", terminationFuture, null)));
        JobManagerRunner jobManagerRunner =
                createJobManagerLeadershipRunner(jobMasterServiceProcessFactory);

        jobManagerRunner.start();

        leaderElectionService.isLeader(UUID.randomUUID()).get();

        terminationFuture.completeExceptionally(
                new FlinkException("The JobMasterService failed unexpectedly."));

        assertThat(
                jobManagerRunner.getResultFuture(),
                FlinkMatchers.futureWillCompleteExceptionally(Duration.ofSeconds(10L)));
    }

    /*  @Test
    public void testJobMasterCreationFailureCompletesJobManagerRunnerWithInitializationError()
            throws Exception {

        final FlinkException testException = new FlinkException("Test exception");
        final TestingJobMasterServiceFactory jobMasterServiceFactory =
                new TestingJobMasterServiceFactory(
                        () -> {
                            throw testException;
                        });

        final JobManagerRunner jobManagerRunner =
                createJobManagerLeadershipRunner(jobMasterServiceFactory);

        jobManagerRunner.start();

        leaderElectionService.isLeader(UUID.randomUUID());

        final JobManagerRunnerResult jobManagerRunnerResult =
                jobManagerRunner.getResultFuture().join();
        assertTrue(jobManagerRunnerResult.isInitializationFailure());
        assertTrue(
                jobManagerRunnerResult.getInitializationFailure()
                        instanceof JobInitializationException);
        assertThat(jobManagerRunnerResult.getInitializationFailure(), containsCause(testException));
    }

    @Test
    public void testJobMasterShutDownOnRunnerShutdownDuringJobMasterInitialization()
            throws Exception {
        final BlockingJobMasterServiceFactory blockingJobMasterServiceFactory =
                new BlockingJobMasterServiceFactory();

        final JobManagerRunner jobManagerRunner =
                createJobManagerLeadershipRunner(blockingJobMasterServiceFactory);

        jobManagerRunner.start();

        leaderElectionService.isLeader(UUID.randomUUID());

        TestingJobMasterService testingJobMasterService =
                blockingJobMasterServiceFactory.waitForBlockingOnInit();

        CompletableFuture<Void> closeFuture = jobManagerRunner.closeAsync();

        blockingJobMasterServiceFactory.unblock();

        closeFuture.get();

        assertJobNotFinished(jobManagerRunner.getResultFuture());

        assertThat(testingJobMasterService.isClosed(), is(true));
    }

    @Test
    public void testJobMasterShutdownOnLeadershipLossDuringInitialization() throws Exception {
        final BlockingJobMasterServiceFactory blockingJobMasterServiceFactory =
                new BlockingJobMasterServiceFactory();

        final JobManagerRunner jobManagerRunner =
                createJobManagerLeadershipRunner(blockingJobMasterServiceFactory);

        jobManagerRunner.start();

        leaderElectionService.isLeader(UUID.randomUUID());

        TestingJobMasterService testingJobMasterService =
                blockingJobMasterServiceFactory.waitForBlockingOnInit();

        leaderElectionService.notLeader();

        blockingJobMasterServiceFactory.unblock();

        // assert termination of testingJobMaster
        testingJobMasterService.getTerminationFuture().get();
        assertThat(testingJobMasterService.isClosed(), is(true));
    }

    @Test
    public void testJobCancellationOnCancellationDuringInitialization() throws Exception {
        AtomicBoolean cancelCalled = new AtomicBoolean(false);
        JobMasterGateway jobMasterGateway =
                new TestingJobMasterGatewayBuilder()
                        .setCancelFunction(
                                () -> {
                                    cancelCalled.set(true);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .build();

        TestingJobMasterService testingJobMasterService =
                new TestingJobMasterService(jobMasterGateway);
        final BlockingJobMasterServiceFactory blockingJobMasterServiceFactory =
                new BlockingJobMasterServiceFactory(() -> testingJobMasterService);

        final JobManagerRunner jobManagerRunner =
                createJobManagerLeadershipRunner(blockingJobMasterServiceFactory);

        jobManagerRunner.start();

        leaderElectionService.isLeader(UUID.randomUUID());

        blockingJobMasterServiceFactory.waitForBlockingOnInit();

        // cancel during init
        CompletableFuture<Acknowledge> cancellationFuture =
                jobManagerRunner.cancel(TESTING_TIMEOUT);

        assertThat(cancellationFuture.isDone(), is(false));

        blockingJobMasterServiceFactory.unblock();

        // assert that cancellation future completes when cancellation completes.
        cancellationFuture.get();
        assertThat(cancelCalled.get(), is(true));
    }

    @Test
    public void testJobInformationOperationsDuringInitialization() throws Exception {
        final BlockingJobMasterServiceFactory blockingJobMasterServiceFactory =
                new BlockingJobMasterServiceFactory();

        final JobManagerRunner jobManagerRunner =
                createJobManagerLeadershipRunner(blockingJobMasterServiceFactory);

        jobManagerRunner.start();

        // assert initializing while waiting for leadership
        assertInitializingStates(jobManagerRunner);

        // assign leadership
        leaderElectionService.isLeader(UUID.randomUUID());

        // assert initializing while JobMaster is blocked
        assertInitializingStates(jobManagerRunner);
        blockingJobMasterServiceFactory.unblock();
    } */

    private static void assertInitializingStates(JobManagerRunner jobManagerRunner)
            throws ExecutionException, InterruptedException {
        assertThat(
                jobManagerRunner.requestJobStatus(TESTING_TIMEOUT).get(),
                is(JobStatus.INITIALIZING));
        assertThat(jobManagerRunner.getResultFuture().isDone(), is(false));
        assertThat(
                jobManagerRunner
                        .requestJob(TESTING_TIMEOUT)
                        .get()
                        .getArchivedExecutionGraph()
                        .getState(),
                is(JobStatus.INITIALIZING));

        assertThat(
                jobManagerRunner.requestJobDetails(TESTING_TIMEOUT).get().getStatus(),
                is(JobStatus.INITIALIZING));
    }

    @Test
    public void testShutdownInInitializedState() throws Exception {
        final JobManagerLeadershipRunner jobManagerRunner = createJobManagerLeadershipRunner();
        jobManagerRunner.start();
        // grant leadership to finish initialization
        leaderElectionService.isLeader(UUID.randomUUID()).get();

        assertThat(jobManagerRunner.isInitialized(), is(true));

        jobManagerRunner.close();

        assertJobNotFinished(jobManagerRunner.getResultFuture());
    }

    /* @Test
    public void testShutdownWhileWaitingForCancellationDuringInitialization() throws Exception {
        final BlockingJobMasterServiceFactory blockingJobMasterServiceFactory =
                new BlockingJobMasterServiceFactory();

        final JobManagerRunner jobManagerRunner =
                createJobManagerLeadershipRunner(blockingJobMasterServiceFactory);

        jobManagerRunner.start();

        leaderElectionService.isLeader(UUID.randomUUID());

        blockingJobMasterServiceFactory.waitForBlockingOnInit();

        // cancel while initializing
        assertThat(
                jobManagerRunner.requestJobStatus(TESTING_TIMEOUT).get(),
                is(JobStatus.INITIALIZING));

        CompletableFuture<Acknowledge> cancelFuture = jobManagerRunner.cancel(TESTING_TIMEOUT);
        assertThat(cancelFuture.isDone(), is(false));

        CompletableFuture<Void> closeFuture = jobManagerRunner.closeAsync();
        assertThat(closeFuture.isDone(), is(false));

        // the close operation finishes only once the initialization finishes
        blockingJobMasterServiceFactory.unblock();

        assertThat(cancelFuture.isCompletedExceptionally(), is(true));
        assertJobNotFinished(jobManagerRunner.getResultFuture());
    }

    @Test
    public void testCancellationAfterInitialization() throws Exception {
        AtomicBoolean cancelCalled = new AtomicBoolean(false);
        JobMasterGateway testingGateway =
                new TestingJobMasterGatewayBuilder()
                        .setCancelFunction(
                                () -> {
                                    cancelCalled.set(true);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .build();
        TestingJobMasterServiceFactory jobMasterServiceFactory =
                new TestingJobMasterServiceFactory(
                        () -> new TestingJobMasterService(testingGateway));
        final JobManagerRunnerImpl jobManagerRunner =
                createJobManagerLeadershipRunner(jobMasterServiceFactory);
        jobManagerRunner.start();
        // grant leadership to finish initialization
        leaderElectionService.isLeader(UUID.randomUUID()).get();

        assertThat(jobManagerRunner.isInitialized(), is(true));

        jobManagerRunner.cancel(TESTING_TIMEOUT).get();
        assertThat(cancelCalled.get(), is(true));
    }

    // It can happen that a series of leadership operations happens while the JobMaster
    // initialization is blocked. This test is to ensure that we are not starting-stopping
    // JobMasters for all pending leadership grants, but only for the latest.
    @Test
    public void testSkippingOfEnqueuedLeadershipOperations() throws Exception {
        final BlockingJobMasterServiceFactory blockingJobMasterServiceFactory =
                new BlockingJobMasterServiceFactory();

        final JobManagerRunner jobManagerRunner =
                createJobManagerLeadershipRunner(blockingJobMasterServiceFactory);

        jobManagerRunner.start();

        // first leadership assignment to get into blocking initialization
        leaderElectionService.isLeader(UUID.randomUUID());

        blockingJobMasterServiceFactory.waitForBlockingOnInit();

        // we are now blocked on the initialization, enqueue some operations:
        for (int i = 0; i < 10; i++) {
            leaderElectionService.notLeader();
            leaderElectionService.isLeader(UUID.randomUUID());
        }

        blockingJobMasterServiceFactory.unblock();

        // wait until the second JobMaster has been created
        blockingJobMasterServiceFactory.waitForBlockingOnInit();

        assertThat(
                blockingJobMasterServiceFactory.getNumberOfJobMasterInstancesCreated(), equalTo(2));
    }

    @Test
    public void testCancellationFailsWhenInitializationFails() throws Exception {
        final BlockingJobMasterServiceFactory blockingJobMasterServiceFactory =
                new BlockingJobMasterServiceFactory();

        final JobManagerRunner jobManagerRunner =
                createJobManagerLeadershipRunner(blockingJobMasterServiceFactory);

        jobManagerRunner.start();

        leaderElectionService.isLeader(UUID.randomUUID());

        blockingJobMasterServiceFactory.waitForBlockingOnInit();

        // cancel while initializing
        assertThat(
                jobManagerRunner.requestJobStatus(TESTING_TIMEOUT).get(),
                is(JobStatus.INITIALIZING));

        CompletableFuture<Acknowledge> cancelFuture = jobManagerRunner.cancel(TESTING_TIMEOUT);
        assertThat(cancelFuture.isDone(), is(false));

        blockingJobMasterServiceFactory.failBlockingInitialization();

        try {
            cancelFuture.get();
            fail();
        } catch (Throwable t) {
            assertThat(
                    t,
                    containsMessage("Cancellation failed because JobMaster initialization failed"));
        }
        assertThat(jobManagerRunner.getResultFuture().get().isInitializationFailure(), is(true));
    } */

    private void assertJobNotFinished(CompletableFuture<JobManagerRunnerResult> resultFuture)
            throws ExecutionException, InterruptedException {

        JobManagerRunnerResult jobManagerResult = resultFuture.get();
        assertThat(jobManagerResult.isJobNotFinished(), is(true));
    }

    @Nonnull
    private JobManagerLeadershipRunner createJobManagerLeadershipRunner(
            LibraryCacheManager.ClassLoaderLease classLoaderLease) throws Exception {
        return createJobManagerLeadershipRunner(
                defaultJobMasterServiceProcessFactory, classLoaderLease);
    }

    @Nonnull
    private JobManagerLeadershipRunner createJobManagerLeadershipRunner() throws Exception {
        return createJobManagerLeadershipRunner(
                defaultJobMasterServiceProcessFactory,
                TestingClassLoaderLease.newBuilder().build());
    }

    @Nonnull
    private JobManagerLeadershipRunner createJobManagerLeadershipRunner(
            JobMasterServiceProcessFactory jobMasterServiceProcessFactory) throws Exception {
        return createJobManagerLeadershipRunner(
                jobMasterServiceProcessFactory, TestingClassLoaderLease.newBuilder().build());
    }

    @Nonnull
    JobManagerLeadershipRunner createJobManagerLeadershipRunner(
            JobMasterServiceProcessFactory jobMasterServiceProcessFactory,
            LibraryCacheManager.ClassLoaderLease classLoaderLease)
            throws Exception {

        return new JobManagerLeadershipRunner(
                jobGraph,
                jobMasterServiceProcessFactory,
                haServices,
                classLoaderLease,
                fatalErrorHandler,
                System.currentTimeMillis());
    }

    /*  public static class BlockingJobMasterServiceFactory implements JobMasterServiceFactory {

        private final OneShotLatch blocker = new OneShotLatch();
        private final BlockingQueue<TestingJobMasterService> jobMasterServicesQueue =
                new ArrayBlockingQueue(1);
        private final Supplier<TestingJobMasterService> testingJobMasterServiceSupplier;
        private int numberOfJobMasterInstancesCreated = 0;
        private FlinkException initializationException = null;

        public BlockingJobMasterServiceFactory() {
            this((JobMasterGateway) null);
        }

        public BlockingJobMasterServiceFactory(@Nullable JobMasterGateway jobMasterGateway) {
            this(() -> new TestingJobMasterService(null, null, jobMasterGateway));
        }

        public BlockingJobMasterServiceFactory(
                Supplier<TestingJobMasterService> testingJobMasterServiceSupplier) {
            this.testingJobMasterServiceSupplier = testingJobMasterServiceSupplier;
        }

        @Override
        public JobMasterService createJobMasterService(
                JobGraph jobGraph,
                JobMasterId jobMasterId,
                OnCompletionActions jobCompletionActions,
                ClassLoader userCodeClassloader,
                long initializationTimestamp)
                throws Exception {
            TestingJobMasterService service = testingJobMasterServiceSupplier.get();
            jobMasterServicesQueue.offer(service);

            blocker.await();
            if (initializationException != null) {
                throw initializationException;
            }
            numberOfJobMasterInstancesCreated++;
            return service;
        }

        public void unblock() {
            blocker.trigger();
        }

        public TestingJobMasterService waitForBlockingOnInit()
                throws ExecutionException, InterruptedException {
            return jobMasterServicesQueue.take();
        }

        public int getNumberOfJobMasterInstancesCreated() {
            return numberOfJobMasterInstancesCreated;
        }

        public void failBlockingInitialization() {
            Preconditions.checkState(
                    !blocker.isTriggered(),
                    "This only works before the initialization has been unblocked");
            this.initializationException =
                    new FlinkException("Test exception during initialization");
            unblock();
        }
    } */
}
