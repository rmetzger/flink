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
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerRunnerResult;
import org.apache.flink.runtime.jobmaster.JobManagerStatusListener;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobNotFinishedException;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.concurrent.CompletableFuture;

/** Abstraction used by the {@link Dispatcher} to manage jobs. */
public final class DispatcherJob implements AutoCloseableAsync, JobManagerStatusListener {

    private final Logger log = LoggerFactory.getLogger(DispatcherJob.class);

    private CompletableFuture<DispatcherJobResult> jobResultFuture;

    // if the termination future is set, we are signaling that this DispatcherJob is closing / has
    // been closed
    @GuardedBy("lock")
    @Nullable
    private CompletableFuture<Void> terminationFuture;

    private final long initializationTimestamp;
    private final JobID jobId;
    private final String jobName;

    // We need to guard access to the status field, because onJobManagerStarted() gets called from
    // an executor pool thread.
    private final Object lock = new Object();

    @GuardedBy("lock")
    private final DispatcherJobStatus jobStatus = new DispatcherJobStatus();

    static DispatcherJob createFor(JobID jobId, String jobName, long initializationTimestamp) {
        return new DispatcherJob(jobId, jobName, initializationTimestamp);
    }

    private DispatcherJob(JobID jobId, String jobName, long initializationTimestamp) {
        this.jobId = jobId;
        this.jobName = jobName;
        this.initializationTimestamp = initializationTimestamp;
        this.jobResultFuture = new CompletableFuture<>();
    }

    @Override
    public void onJobManagerStarted(JobManagerRunner jobManagerRunner) {
        synchronized (lock) {
            if (terminationFuture != null) {
                // we are awaiting a termination for this DispatcherJob.
                FutureUtils.forward(jobManagerRunner.closeAsync(), terminationFuture);
            }
            jobStatus.setJobManagerCreated(jobManagerRunner);
        }

        jobManagerRunner
                .getResultFuture()
                .whenComplete(
                        (jobManagerRunnerResult, resultThrowable) -> {
                            if (jobManagerRunnerResult != null) {
                                handleJobManagerRunnerResult(jobManagerRunnerResult);
                            } else {
                                jobResultFuture.completeExceptionally(
                                        ExceptionUtils.stripCompletionException(resultThrowable));
                            }
                        });
    }

    private void handleJobManagerRunnerResult(JobManagerRunnerResult jobManagerRunnerResult) {
        if (jobManagerRunnerResult.isSuccess()) {
            jobResultFuture.complete(
                    DispatcherJobResult.forSuccess(jobManagerRunnerResult.getExecutionGraphInfo()));
        } else {
            Preconditions.checkState(jobManagerRunnerResult.isJobNotFinished());
            jobResultFuture.completeExceptionally(new JobNotFinishedException(jobId));
        }
    }

    @Override
    public void onJobManagerStopped() {
        synchronized (lock) {
            if (terminationFuture != null) {
                // This DispatcherJob is terminated
                return;
            }
            jobStatus.setInitializing();
        }
    }

    @Override
    public void onJobManagerInitializationFailed(Throwable initializationFailure) {
        synchronized (lock) {
            Preconditions.checkState(
                    jobStatus.isInitializing(),
                    "Initialization can only fail while being in state initializing");
            jobStatus.setJobManagerCreationFailed(initializationFailure);
            if (terminationFuture != null) {
                // This DispatcherJob is waiting for a termination.
                terminationFuture.complete(null);
            }
        }

        ArchivedExecutionGraph archivedExecutionGraph =
                ArchivedExecutionGraph.createFromInitializingJob(
                        jobId,
                        jobName,
                        JobStatus.FAILED,
                        initializationFailure,
                        initializationTimestamp);
        jobResultFuture.complete(
                DispatcherJobResult.forInitializationFailure(
                        new ExecutionGraphInfo(archivedExecutionGraph), initializationFailure));
    }

    public CompletableFuture<DispatcherJobResult> getResultFuture() {
        return jobResultFuture;
    }

    public CompletableFuture<JobDetails> requestJobDetails(Time timeout) {
        return requestJob(timeout)
                .thenApply(
                        executionGraphInfo -> {
                            synchronized (lock) {
                                return JobDetails.createDetailsForJob(
                                        executionGraphInfo.getArchivedExecutionGraph());
                            }
                        });
    }

    /**
     * Cancel job. A cancellation will be scheduled if the initialization is not completed. The
     * returned future will complete exceptionally if the JobManagerRunner initialization failed.
     */
    public CompletableFuture<Acknowledge> cancel(Time timeout) {
        synchronized (lock) {
            if (jobStatus.isJobManagerCreatedOrFailed()) {
                return getJobMasterGateway()
                        .thenCompose(jobMasterGateway -> jobMasterGateway.cancel(timeout));
            } else {
                log.info(
                        "Cancellation during initialization requested for job {}. Job will be cancelled once JobManager has been initialized.",
                        jobId);
                jobStatus.setCancelling();
                // cancel job
                CompletableFuture<Acknowledge> cancelFuture =
                        getJobMasterGateway()
                                .thenCompose(jobMasterGateway -> jobMasterGateway.cancel(timeout));
                cancelFuture.whenComplete(
                        (ignored, cancelThrowable) -> {
                            synchronized (lock) {
                                if (terminationFuture != null) {
                                    // This DispatcherJob is pending a termination. Forward
                                    // cancellation result.
                                    FutureUtils.forward(
                                            cancelFuture.thenCompose((ign) -> null),
                                            terminationFuture);
                                }
                                if (cancelThrowable != null) {
                                    log.warn(
                                            "Cancellation of job {} failed",
                                            jobId,
                                            cancelThrowable);
                                }
                            }
                        });
                return cancelFuture;
            }
        }
    }

    public CompletableFuture<JobStatus> requestJobStatus(Time timeout) {
        return requestJob(timeout)
                .thenApply(
                        executionGraphInfo ->
                                executionGraphInfo.getArchivedExecutionGraph().getState());
    }

    boolean isJobManagerCreatedOrFailed() {
        synchronized (lock) {
            return jobStatus.isJobManagerCreatedOrFailed();
        }
    }

    /** Returns a future completing to the ExecutionGraphInfo of the job. */
    public CompletableFuture<ExecutionGraphInfo> requestJob(Time timeout) {
        synchronized (lock) {
            if (jobStatus.isJobManagerCreatedOrFailed()) {
                if (jobResultFuture.isDone()) { // job is not running anymore
                    return jobResultFuture.thenApply(DispatcherJobResult::getExecutionGraphInfo);
                }
                return getJobMasterGateway()
                        .thenCompose(jobMasterGateway -> jobMasterGateway.requestJob(timeout));
            } else {
                Preconditions.checkState(jobStatus.isInitializing() || jobStatus.isCancelling());
                return CompletableFuture.completedFuture(
                        new ExecutionGraphInfo(
                                ArchivedExecutionGraph.createFromInitializingJob(
                                        jobId,
                                        jobName,
                                        jobStatus.asJobStatus(),
                                        null,
                                        initializationTimestamp)));
            }
        }
    }

    /**
     * Returns the {@link JobMasterGateway} from the JobManagerRunner.
     *
     * @return the {@link JobMasterGateway}. The future will complete exceptionally if the
     *     JobManagerRunner initialization failed.
     * @throws IllegalStateException is thrown if the job is not initialized
     */
    public CompletableFuture<JobMasterGateway> getJobMasterGateway() {
        synchronized (lock) {
            return jobStatus
                    .getJobManagerRunnerFuture()
                    .thenCompose(JobManagerRunner::getJobMasterGateway);
        }
    }

    /**
     * Closes this DispatcherJob instance. If the JobManager has been initialized already, we close
     * it, otherwise, we wait for the initialization to be finished, and then close it.
     *
     * @return A future which completes when the DispatcherJob has closed.
     */
    @Override
    public CompletableFuture<Void> closeAsync() {
        synchronized (lock) {
            if (terminationFuture == null) {
                terminationFuture = new CompletableFuture<>();
            }
            if (jobStatus.isJobManagerCreatedOrFailed()) {
                final CompletableFuture<JobManagerRunner> jobManagerRunnerFuture =
                        jobStatus.getJobManagerRunnerFuture();
                if (jobManagerRunnerFuture.isCompletedExceptionally()) {
                    // initialization has failed.
                    return CompletableFuture.completedFuture(null);
                } else {
                    // JobManager is running: close it.
                    return jobManagerRunnerFuture.thenCompose(AutoCloseableAsync::closeAsync);
                }
            } else {
                return terminationFuture;
            }
        }
    }
}
