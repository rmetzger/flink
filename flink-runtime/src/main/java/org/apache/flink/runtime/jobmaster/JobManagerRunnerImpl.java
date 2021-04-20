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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmaster.factories.JobMasterServiceFactory;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The runner for the job manager. It deals with job level leader election and make underlying job
 * manager properly reacted.
 */
public class JobManagerRunnerImpl
        implements LeaderContender, OnCompletionActions, JobManagerRunner {

    private static final Logger log = LoggerFactory.getLogger(JobManagerRunnerImpl.class);

    // ------------------------------------------------------------------------

    /**
     * Lock to ensure that this runner can deal with leader election event and job completion
     * notifies simultaneously.
     */
    private final Object lock = new Object();

    /** The job graph needs to run. */
    private final JobGraph jobGraph;

    /** Used to check whether a job needs to be run. */
    private final RunningJobsRegistry runningJobsRegistry;

    /** Leader election for this job. */
    private final LeaderElectionService leaderElectionService;

    private final LibraryCacheManager.ClassLoaderLease classLoaderLease;

    private final Executor executor;

    private final FatalErrorHandler fatalErrorHandler;

    private final CompletableFuture<JobManagerRunnerResult> resultFuture;

    private final JobMasterServiceFactory jobMasterServiceFactory;

    private final ClassLoader userCodeClassLoader;

    private final long initializationTimestamp;

    private CompletableFuture<Void> leadershipOperation;

    private volatile CompletableFuture<JobMasterGateway> leaderGatewayFuture;

    @GuardedBy("lock")
    // private JobManagerRunnerJobStatus jobStatus = JobManagerRunnerJobStatus.INITIALIZING;
    private JobManagerRunnerImplState runnerState = new WaitForLeadership();

    @GuardedBy("lock")
    @Nullable
    private UUID currentLeaderSession = null;

    // ------------------------------------------------------------------------

    /**
     * Exceptions that occur while creating the JobManager or JobManagerRunnerImpl are directly
     * thrown and not reported to the given {@code FatalErrorHandler}.
     *
     * @throws Exception Thrown if the runner cannot be set up, because either one of the required
     *     services could not be started, or the Job could not be initialized.
     */
    public JobManagerRunnerImpl(
            final JobGraph jobGraph,
            final JobMasterServiceFactory jobMasterServiceFactory,
            final HighAvailabilityServices haServices,
            final LibraryCacheManager.ClassLoaderLease classLoaderLease,
            final Executor executor,
            final FatalErrorHandler fatalErrorHandler,
            long initializationTimestamp)
            throws Exception {

        this.resultFuture = new CompletableFuture<>();
        this.leadershipOperation = CompletableFuture.completedFuture(null);

        this.jobGraph = checkNotNull(jobGraph);
        this.jobMasterServiceFactory = checkNotNull(jobMasterServiceFactory);
        this.classLoaderLease = checkNotNull(classLoaderLease);
        this.executor = checkNotNull(executor);
        this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
        this.initializationTimestamp = initializationTimestamp;

        checkArgument(jobGraph.getNumberOfVertices() > 0, "The given job is empty");

        // libraries and class loader first
        try {
            userCodeClassLoader =
                    classLoaderLease
                            .getOrResolveClassLoader(
                                    jobGraph.getUserJarBlobKeys(), jobGraph.getClasspaths())
                            .asClassLoader();
        } catch (IOException e) {
            throw new Exception("Cannot set up the user code libraries: " + e.getMessage(), e);
        }

        // high availability services next
        this.runningJobsRegistry = haServices.getRunningJobsRegistry();
        this.leaderElectionService =
                haServices.getJobManagerLeaderElectionService(jobGraph.getJobID());

        this.leaderGatewayFuture = new CompletableFuture<>();
    }

    // ----------------------------------------------------------------------------------------------
    // Getter
    // ----------------------------------------------------------------------------------------------

    @Override
    public CompletableFuture<JobMasterGateway> getJobMasterGateway() {
        return leaderGatewayFuture;
    }

    @Override
    public CompletableFuture<JobManagerRunnerResult> getResultFuture() {
        return resultFuture;
    }

    @Override
    public JobID getJobID() {
        return jobGraph.getJobID();
    }

    // ----------------------------------------------------------------------------------------------
    // Lifecycle management
    // ----------------------------------------------------------------------------------------------

    @Override
    public void start() throws Exception {
        try {
            leaderElectionService.start(this);
        } catch (Exception e) {
            log.error(
                    "Could not start the JobManager because the leader election service did not start.",
                    e);
            throw new Exception("Could not start the leader election service.", e);
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        synchronized (lock) {
            return runnerState.closeAsync();
        }
        /*  synchronized (lock) {
            if (!shutdown) {
                shutdown = true;

                setNewLeaderGatewayFuture();
                final FlinkException shutdownException =
                        new FlinkException("JobMaster has been shut down.");
                leaderGatewayFuture.completeExceptionally(shutdownException);

                if (jobStatus == JobManagerRunnerJobStatus.JOBMASTER_INITIALIZED) {
                    checkState(
                            jobMasterService != null,
                            "JobMaster service must be set when Job master is initialized");
                    FutureUtils.assertNoException(
                            jobMasterService
                                    .closeAsync()
                                    .whenComplete(
                                            (Void ignored, Throwable throwable) ->
                                                    onJobManagerTermination(throwable)));
                } else if (jobStatus.isInitializing()) {
                    if (jobStatus == JobManagerRunnerJobStatus.INITIALIZING_CANCELLING) {
                        checkState(cancelFuture != null);
                        cancelFuture.completeExceptionally(shutdownException);
                    }

                    if (currentLeaderSession == null) {
                        // no ongoing JobMaster initialization (waiting for leadership) --> close
                        onJobManagerTermination(null);
                    }
                    // ongoing initialization, we will finish closing once it is done.
                }
            }

            return terminationFuture;
        } */
    }

    /*  private void onJobManagerTermination(@Nullable Throwable throwable) {
        try {
            leaderElectionService.stop();
        } catch (Throwable t) {
            throwable =
                    ExceptionUtils.firstOrSuppressed(
                            t, ExceptionUtils.stripCompletionException(throwable));
        }

        classLoaderLease.release();

        resultFuture.completeExceptionally(new JobNotFinishedException(jobGraph.getJobID()));

        if (throwable != null) {
            terminationFuture.completeExceptionally(
                    new FlinkException(
                            "Could not properly shut down the JobManagerRunner", throwable));
        } else {
            terminationFuture.complete(null);
        }
    } */

    // ----------------------------------------------------------------------------------------------
    // Job operations
    // ----------------------------------------------------------------------------------------------

    @Override
    public CompletableFuture<Acknowledge> cancel(Time timeout) {
        synchronized (lock) {
            return runnerState.cancel(timeout);
        }
        /*synchronized (lock) {
            if (jobStatus == JobManagerRunnerJobStatus.JOBMASTER_INITIALIZED) {
                return leaderGatewayFuture.thenCompose(
                        jobMasterGateway -> jobMasterGateway.cancel(timeout));
            } else {
                if (jobStatus == JobManagerRunnerJobStatus.INITIALIZING_CANCELLING) {
                    checkState(cancelFuture != null);
                    return cancelFuture;
                }
                log.info(
                        "Cancellation during initialization requested for job {}. Job will be cancelled once JobManager has been initialized.",
                        jobGraph.getJobID());

                cancelFuture = new CompletableFuture<>();
                cancelTimeout = timeout;
                jobStatus = JobManagerRunnerJobStatus.INITIALIZING_CANCELLING;
                return cancelFuture;
            }
        } */
    }

    @Override
    public CompletableFuture<JobStatus> requestJobStatus(Time timeout) {
        return requestJob(timeout)
                .thenApply(
                        executionGraphInfo ->
                                executionGraphInfo.getArchivedExecutionGraph().getState());
    }

    @Override
    public CompletableFuture<JobDetails> requestJobDetails(Time timeout) {
        return requestJob(timeout)
                .thenApply(
                        executionGraphInfo ->
                                JobDetails.createDetailsForJob(
                                        executionGraphInfo.getArchivedExecutionGraph()));
    }

    @Override
    public CompletableFuture<ExecutionGraphInfo> requestJob(Time timeout) {
        synchronized (lock) {
            return runnerState.requestJob(timeout);
        }
        /*  synchronized (lock) {
            if (jobStatus == JobManagerRunnerJobStatus.JOBMASTER_INITIALIZED) {
                if (resultFuture.isDone()) { // job is not running anymore
                    return resultFuture.thenApply(JobManagerRunnerResult::getExecutionGraphInfo);
                }
                // job is running
                return leaderGatewayFuture.thenCompose(
                        jobMasterGateway -> jobMasterGateway.requestJob(timeout));
            } else {
                checkState(jobStatus.isInitializing());
                return CompletableFuture.completedFuture(
                        new ExecutionGraphInfo(
                                ArchivedExecutionGraph.createFromInitializingJob(
                                        jobGraph.getJobID(),
                                        jobGraph.getName(),
                                        jobStatus.asJobStatus(),
                                        null,
                                        initializationTimestamp)));
            }
        } */
    }

    @Override
    public boolean isInitialized() {
        synchronized (lock) {
            return runnerState instanceof JobMasterRunning;
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Result and error handling methods
    // ----------------------------------------------------------------------------------------------

    /** Job completion notification triggered by JobManager. */
    @Override
    public void jobReachedGloballyTerminalState(ExecutionGraphInfo executionGraphInfo) {
        unregisterJobFromHighAvailability();
        // complete the result future with the information of the information of the terminated job
        resultFuture.complete(JobManagerRunnerResult.forSuccess(executionGraphInfo));
    }

    /** Job completion notification triggered by self. */
    @Override
    public void jobFinishedByOther() {
        resultFuture.completeExceptionally(new JobNotFinishedException(jobGraph.getJobID()));
    }

    @Override
    public void jobMasterFailed(Throwable cause) {
        handleJobManagerRunnerError(cause);
    }

    private void handleJobManagerRunnerError(Throwable cause) {
        if (ExceptionUtils.isJvmFatalError(cause)) {
            fatalErrorHandler.onFatalError(cause);
        } else {
            resultFuture.completeExceptionally(cause);
        }
    }

    /**
     * Marks this runner's job as not running. Other JobManager will not recover the job after this
     * call.
     *
     * <p>This method never throws an exception.
     */
    private void unregisterJobFromHighAvailability() {
        try {
            runningJobsRegistry.setJobFinished(jobGraph.getJobID());
        } catch (Throwable t) {
            log.error(
                    "Could not un-register from high-availability services job {} ({})."
                            + "Other JobManager's may attempt to recover it and re-execute it.",
                    jobGraph.getName(),
                    jobGraph.getJobID(),
                    t);
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Leadership methods
    // ----------------------------------------------------------------------------------------------

    @Override
    public void grantLeadership(final UUID leaderSessionID) {
        synchronized (lock) {
            if (runnerState.acceptsLeadershipOperations()) {
                log.debug(
                        "JobManagerRunner cannot be granted leadership because it is in state {}",
                        runnerState.getClass().getSimpleName());
                return;
            }
            this.currentLeaderSession = leaderSessionID;

            // enqueue a leadership operation
            leadershipOperation =
                    leadershipOperation.thenApply(
                            (ign) -> {
                                synchronized (lock) {
                                    if (currentLeaderSession == null
                                            || !currentLeaderSession.equals(leaderSessionID)) {
                                        // lost leadership in the meantime, complete this operation
                                        return null;
                                    }
                                    if (runnerState instanceof WaitForLeadership) {
                                        try {
                                            ((WaitForLeadership) runnerState)
                                                    .grantLeadership(leaderSessionID);
                                        } catch (FlinkException e) {
                                            ExceptionUtils.rethrow(e);
                                        }
                                    }
                                    // if we are not in state WaitForLeadership, we'll pick up the
                                    // currentLeaderSession.
                                }
                                return null;
                            });
            handleException(leadershipOperation, "Could not start the job manager.");
        }
    }

    // JobMaster initialization is completed. Ensure proper state and leadership
    /* @GuardedBy("lock")
    private CompletableFuture<Void> onJobMasterInitializationCompletion(
            JobMasterService newJobMasterService, UUID leaderSessionId) {
        checkNotNull(newJobMasterService);
        if (shutdown) {
            return newJobMasterService
                    .closeAsync()
                    .whenComplete(
                            (Void ignored, Throwable throwable) ->
                                    onJobManagerTermination(throwable));
        }

        checkState(
                jobStatus.isInitializing(),
                "Can only complete initialization in initializing state");

        if (leaderElectionService.hasLeadership(leaderSessionId)) {
            jobMasterService = newJobMasterService;
            leaderGatewayFuture.complete(jobMasterService.getGateway());
            leaderElectionService.confirmLeadership(leaderSessionId, jobMasterService.getAddress());

            if (jobStatus == JobManagerRunnerJobStatus.INITIALIZING_CANCELLING) {
                checkState(cancelFuture != null);
                FutureUtils.forward(
                        newJobMasterService.getGateway().cancel(cancelTimeout), cancelFuture);
            }

            jobStatus = JobManagerRunnerJobStatus.JOBMASTER_INITIALIZED;
        } else {
            log.info(
                    "Ignoring confirmation of leader session id because {} is no longer the leader. Shutting down JobMaster",
                    getDescription());
            return newJobMasterService.closeAsync();
        }

        return CompletableFuture.completedFuture(null);
    } */

    @Nullable
    private JobMasterService startJobMasterServiceSafely(UUID leaderSessionId) throws Exception {
        // We assume this operation to be potentially long-running (thus it can not block the
        // JobManager main thread on it)
        final JobMasterService newJobMasterService =
                jobMasterServiceFactory.createJobMasterService(
                        jobGraph,
                        new JobMasterId(leaderSessionId),
                        this,
                        userCodeClassLoader,
                        initializationTimestamp);

        newJobMasterService
                .getTerminationFuture()
                .whenComplete(
                        (unused, throwable) -> {
                            if (throwable != null) {
                                synchronized (lock) {
                                    // check that we are still running and the JobMasterService
                                    // is still valid
                                    if (!shutdown && newJobMasterService == jobMasterService) {
                                        handleJobManagerRunnerError(throwable);
                                    }
                                }
                            }
                        });
        return newJobMasterService;
    }

    private void jobAlreadyDone() {
        log.info("Granted leader ship but job {} has been finished. ", jobGraph.getJobID());
        jobFinishedByOther();
    }

    private RunningJobsRegistry.JobSchedulingStatus getJobSchedulingStatus() throws FlinkException {
        try {
            return runningJobsRegistry.getJobSchedulingStatus(jobGraph.getJobID());
        } catch (IOException e) {
            throw new FlinkException(
                    String.format(
                            "Could not retrieve the job scheduling status for job %s.",
                            jobGraph.getJobID()),
                    e);
        }
    }

    @Override
    public void revokeLeadership() {
        synchronized (lock) {
            if (runnerState.acceptsLeadershipOperations()) {
                log.debug(
                        "Ignoring revoking leadership because JobManagerRunner is closing or already closed in state {}",
                        runnerState.getClass().getSimpleName());
                return;
            }
            // unset current leader session to fail-fast queued grant leadership operations
            currentLeaderSession = null;

            leadershipOperation =
                    leadershipOperation.thenCompose(
                            (ignored) -> {
                                synchronized (lock) {
                                    runnerState.revokeLeadership();
                                    // TODO
                                    // return revokeJobMasterLeadership();
                                }
                            });

            handleException(leadershipOperation, "Could not suspend the job manager.");
        }
    }

    /*   @GuardedBy("lock")
    private CompletableFuture<Void> revokeJobMasterLeadership() {
        if (shutdown) {
            log.debug(
                    "Ignoring revoking JobMaster leadership because JobManagerRunner is already shut down.");
            return FutureUtils.completedVoidFuture();
        }

        if (jobMasterService != null) {
            log.info(
                    "JobManager for job {} ({}) at {} was revoked leadership.",
                    jobGraph.getName(),
                    jobGraph.getJobID(),
                    jobMasterService.getAddress());
            setNewLeaderGatewayFuture();

            final CompletableFuture<Void> jobMasterServiceTerminationFuture =
                    jobMasterService.closeAsync();
            jobMasterService = null;
            jobStatus = JobManagerRunnerJobStatus.INITIALIZING;

            return jobMasterServiceTerminationFuture;
        } else {
            return FutureUtils.completedVoidFuture();
        }
    } */

    private void handleException(CompletableFuture<Void> leadershipOperation, String message) {
        leadershipOperation.whenComplete(
                (ignored, throwable) -> {
                    if (throwable != null) {
                        handleJobManagerRunnerError(new FlinkException(message, throwable));
                    }
                });
    }

    @GuardedBy("lock")
    private void setNewLeaderGatewayFuture() {
        final CompletableFuture<JobMasterGateway> oldLeaderGatewayFuture = leaderGatewayFuture;

        leaderGatewayFuture = new CompletableFuture<>();

        if (!oldLeaderGatewayFuture.isDone()) {
            leaderGatewayFuture.whenComplete(
                    (JobMasterGateway jobMasterGateway, Throwable throwable) -> {
                        if (throwable != null) {
                            oldLeaderGatewayFuture.completeExceptionally(throwable);
                        } else {
                            oldLeaderGatewayFuture.complete(jobMasterGateway);
                        }
                    });
        }
    }

    @Override
    public void handleError(Exception exception) {
        log.error("Leader Election Service encountered a fatal error.", exception);
        handleJobManagerRunnerError(exception);
    }

    // ------------------------------------------------------------------------

    private interface JobManagerRunnerImplState {
        CompletableFuture<Void> closeAsync();

        CompletableFuture<Acknowledge> cancel(Time timeout);

        void revokeLeadership();

        CompletableFuture<ExecutionGraphInfo> requestJob(Time timeout);

        default boolean acceptsLeadershipOperations() {
            return false;
        }

        JobStatus getJobStatus();
    }

    private final class WaitForLeadership implements JobManagerRunnerImplState {

        private WaitForLeadership() throws FlinkException {
            if (currentLeaderSession != null) {
                grantLeadership(currentLeaderSession);
            }
        }

        @GuardedBy("lock")
        @Override
        public CompletableFuture<Void> closeAsync() {
            CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
            runnerState = new Closed(terminationFuture);
            return terminationFuture;
        }

        @GuardedBy("lock")
        @Override
        public CompletableFuture<Acknowledge> cancel(Time timeout) {
            CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
            runnerState = new Closed(terminationFuture);
            return terminationFuture.thenApply((ign) -> Acknowledge.get());
        }

        @GuardedBy("lock")
        public void grantLeadership(UUID leaderSessionId) throws FlinkException {
            runnerState = new WaitForJobMaster(leaderSessionId);
        }

        @Override
        public void revokeLeadership() {
            throw new IllegalStateException("Leadership revocation while waiting for leadership");
        }

        @GuardedBy("lock")
        @Override
        public CompletableFuture<ExecutionGraphInfo> requestJob(Time timeout) {
            return getExecutionGraphInfoForInitializingJob();
        }

        @Override
        public JobStatus getJobStatus() {
            return JobStatus.INITIALIZING;
        }
    }

    @GuardedBy("lock")
    CompletableFuture<ExecutionGraphInfo> getExecutionGraphInfoForInitializingJob() {
        return CompletableFuture.completedFuture(
                new ExecutionGraphInfo(
                        ArchivedExecutionGraph.createFromInitializingJob(
                                jobGraph.getJobID(),
                                jobGraph.getName(),
                                runnerState.getJobStatus(),
                                null,
                                initializationTimestamp)));
    }

    private class Closed implements JobManagerRunnerImplState {

        private final CompletableFuture<Void> terminationFuture;

        Closed(CompletableFuture<Void> terminationFuture) {
            this.terminationFuture = terminationFuture;
            Throwable throwable = null;
            try {
                leaderElectionService.stop();
            } catch (Throwable t) {
                throwable =
                        ExceptionUtils.firstOrSuppressed(
                                t, ExceptionUtils.stripCompletionException(throwable));
            }

            classLoaderLease.release();

            resultFuture.completeExceptionally(new JobNotFinishedException(jobGraph.getJobID()));

            if (throwable != null) {
                terminationFuture.completeExceptionally(
                        new FlinkException(
                                "Could not properly shut down the JobManagerRunner", throwable));
            } else {
                terminationFuture.complete(null);
            }
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return terminationFuture;
        }

        @Override
        public CompletableFuture<Acknowledge> cancel(Time timeout) {
            throw new IllegalStateException("Unable to cancel job in state closed");
        }

        @Override
        public void revokeLeadership() {
            throw new IllegalStateException();
        }

        @Override
        public CompletableFuture<ExecutionGraphInfo> requestJob(Time timeout) {
            throw new IllegalStateException("Unable to request job information in state closed");
        }

        @Override
        public JobStatus getJobStatus() {
            throw new UnsupportedOperationException();
        }
    }

    private class WaitForJobMaster implements JobManagerRunnerImplState {

        private BiConsumer<JobMasterService, Throwable> onJobMasterCompletionAction =
                transitionToJobMasterRunning();

        public WaitForJobMaster(UUID leaderSessionId) throws FlinkException {
            final RunningJobsRegistry.JobSchedulingStatus jobSchedulingStatus =
                    getJobSchedulingStatus();

            if (jobSchedulingStatus == RunningJobsRegistry.JobSchedulingStatus.DONE) {
                jobAlreadyDone();
            } else {
                startJobMaster(leaderSessionId);
            }
        }

        private void startJobMaster(UUID leaderSessionId) throws FlinkException {
            log.info(
                    "JobManager runner for job {} ({}) was granted leadership with session id {}.",
                    jobGraph.getName(),
                    jobGraph.getJobID(),
                    leaderSessionId);

            try {
                runningJobsRegistry.setJobRunning(jobGraph.getJobID());
            } catch (IOException e) {
                throw new FlinkException(
                        String.format(
                                "Failed to set the job %s to running in the running jobs registry.",
                                jobGraph.getJobID()),
                        e);
            }

            // run blocking JobMaster initialization outside of the lock
            CompletableFuture<JobMasterService> jobMasterStartFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return startJobMasterServiceSafely(leaderSessionId);
                                } catch (Exception e) {
                                    ExceptionUtils.rethrow(e);
                                    return null;
                                }
                            },
                            executor);
            jobMasterStartFuture.whenComplete(
                    (jobMasterService, throwable) -> {
                        synchronized (lock) {
                            onJobMasterCompletionAction.accept(jobMasterService, throwable);
                        }
                    });

            /*  return jobMasterStartFuture
            .handle(
                    (newJobMasterService, initializationError) -> {
                        if (initializationError != null) {
                            synchronized (lock) {
                                // initialization failed
                                if (jobStatus
                                        == JobManagerRunnerJobStatus
                                                .INITIALIZING_CANCELLING) {
                                    checkState(cancelFuture != null);
                                    cancelFuture.completeExceptionally(
                                            new FlinkException(
                                                    "Cancellation failed because JobMaster initialization failed",
                                                    initializationError));
                                }
                                if (shutdown) {
                                    onJobManagerTermination(null);
                                }
                                jobStatus = JobManagerRunnerJobStatus.INITIALIZATION_FAILED;
                                resultFuture.complete(
                                        JobManagerRunnerResult.forInitializationFailure(
                                                new JobInitializationException(
                                                        jobGraph.getJobID(),
                                                        "Could not start the JobMaster.",
                                                        initializationError)));
                            }
                            return null;
                        } else {
                            checkState(newJobMasterService != null);
                            return newJobMasterService;
                        }
                    })
            .thenCompose(
                    (newJobMasterService) -> {
                        synchronized (lock) {
                            if (newJobMasterService != null) {
                                return onJobMasterInitializationCompletion(
                                        newJobMasterService, leaderSessionId);
                            } else {
                                return CompletableFuture.completedFuture(null);
                            }
                        }
                    }); */
        }

        private BiConsumer<JobMasterService, Throwable> transitionToJobMasterRunning() {
            return null;
        }

        @GuardedBy("lock")
        @Override
        public CompletableFuture<Void> closeAsync() {
            WaitForJobMasterClosing waitForClosing = new WaitForJobMasterClosing();
            runnerState = waitForClosing;
            this.onJobMasterCompletionAction =
                    (jobMasterService, throwable) -> {
                        try {
                            waitForClosing.onJobMasterCompletion(jobMasterService, throwable);
                        } catch (FlinkException e) {
                            ExceptionUtils.rethrow(e);
                        }
                    };
            return waitForClosing.getCloseFuture();
        }

        @GuardedBy("lock")
        @Override
        public CompletableFuture<Acknowledge> cancel(Time timeout) {
            WaitForJobMasterCancellation waitForCancellation = new WaitForJobMasterCancellation();
            runnerState = waitForCancellation;
            this.onJobMasterCompletionAction =
                    (jobMasterService, throwable) -> {
                        try {
                            waitForCancellation.onJobMasterCompletion(jobMasterService, throwable);
                        } catch (FlinkException e) {
                            ExceptionUtils.rethrow(e);
                        }
                    };
            return waitForCancellation.getCancelFuture();
        }

        @Override
        public void revokeLeadership() {}

        @Override
        public CompletableFuture<ExecutionGraphInfo> requestJob(Time timeout) {
            return null;
        }

        @Override
        public JobStatus getJobStatus() {
            return JobStatus.INITIALIZING;
        }
    }

    private class WaitForJobMasterClosing implements JobManagerRunnerImplState {
        private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

        @Override
        public CompletableFuture<Void> closeAsync() {
            return closeFuture;
        }

        @Override
        public CompletableFuture<Acknowledge> cancel(Time timeout) {
            return FutureUtils.completedExceptionally(
                    new FlinkException(
                            "Unable to cancel job: This JobMaster is still initializing and is scheduled for being closed"));
        }

        @GuardedBy("lock")
        @Override
        public void revokeLeadership() {
            // TODO: I assume we complete the close operation independent of leader loss. Maybe just
            // log, instead of completing exceptionally
            closeFuture.completeExceptionally(
                    new FlinkException(
                            "Unable to complete JobMaster shutdown because of leader loss"));
        }

        @GuardedBy("lock")
        @Override
        public CompletableFuture<ExecutionGraphInfo> requestJob(Time timeout) {
            return getExecutionGraphInfoForInitializingJob();
        }

        @Override
        public JobStatus getJobStatus() {
            return JobStatus.INITIALIZING;
        }

        @GuardedBy("lock")
        public void onJobMasterCompletion(
                @Nullable JobMasterService jobMasterService, @Nullable Throwable throwable)
                throws FlinkException {
            runnerState = new Closed(closeFuture);
            if (throwable != null) {
                log.debug(
                        "JobMaster initialization failed while waiting for JobManagerRunner to close",
                        throwable);
                closeFuture.complete(null);
            } else {
                checkNotNull(jobMasterService);
                FutureUtils.forward(jobMasterService.closeAsync(), closeFuture);
            }
        }

        public CompletableFuture<Void> getCloseFuture() {
            return closeFuture;
        }
    }

    private class WaitForJobMasterCancellation implements JobManagerRunnerImplState {
        private final CompletableFuture<Acknowledge> cancelFuture = new CompletableFuture<>();
        private boolean hasLeadership = true;
        // If this flag is set, we force-close the JobMaster instance, because a close has been
        // called
        private boolean closed = false;

        @Override
        public CompletableFuture<Void> closeAsync() {
            this.closed = true;
            return cancelFuture.thenApply((ign) -> null);
        }

        @Override
        public CompletableFuture<Acknowledge> cancel(Time timeout) {
            return cancelFuture;
        }

        @GuardedBy("lock")
        @Override
        public void revokeLeadership() {
            hasLeadership = false;
            cancelFuture.completeExceptionally(
                    new FlinkException("Unable to complete cancellation because of leader loss"));
        }

        @GuardedBy("lock")
        @Override
        public CompletableFuture<ExecutionGraphInfo> requestJob(Time timeout) {
            return getExecutionGraphInfoForInitializingJob();
        }

        @GuardedBy("lock")
        public void onJobMasterCompletion(
                @Nullable JobMasterService jobMasterService, @Nullable Throwable throwable)
                throws FlinkException {
            if (!hasLeadership && !closed) {
                // we lost leadership in the meantime
                runnerState = new WaitForLeadership();

                // close jobMaster if it has been created in the meantime
                if (jobMasterService != null) {
                    jobMasterService
                            .closeAsync()
                            .whenComplete(
                                    (ign, throwableFromClose) -> {
                                        if (throwableFromClose != null) {
                                            log.debug(
                                                    "Error while cancelling job which lost leadership during initialization.",
                                                    throwableFromClose);
                                        }
                                    });
                }
                return;
            }
            runnerState = new Closed(cancelFuture.thenApply((ign) -> null));
            // we still have leadership
            if (throwable != null) {
                log.debug(
                        "JobMaster initialization failed while a job cancellation has been requested",
                        throwable);
                cancelFuture.complete(null);
            } else {
                checkNotNull(jobMasterService);
                FutureUtils.forward(
                        jobMasterService.closeAsync().thenApply(ign -> Acknowledge.get()),
                        cancelFuture);
            }
        }

        public CompletableFuture<Acknowledge> getCancelFuture() {
            return cancelFuture;
        }

        @Override
        public JobStatus getJobStatus() {
            return JobStatus.CANCELLING;
        }
    }
}
