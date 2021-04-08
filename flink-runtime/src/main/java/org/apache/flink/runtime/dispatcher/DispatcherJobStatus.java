/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

public class DispatcherJobStatus {
    private Status status;

    private CompletableFuture<JobManagerRunner> jobManagerRunnerFuture = new CompletableFuture<>();

    public DispatcherJobStatus() {
        status = Status.INITIALIZING;
    }

    public void setJobManagerCreated(JobManagerRunner runner) {
        Preconditions.checkState(
                status != Status.JOB_MANAGER_CREATED_OR_INIT_FAILED,
                "JobManager has been created already");
        this.jobManagerRunnerFuture.complete(runner);
        status = Status.JOB_MANAGER_CREATED_OR_INIT_FAILED;
    }

    public void setJobManagerCreationFailed(Throwable failureCause) {
        Preconditions.checkState(
                status != Status.JOB_MANAGER_CREATED_OR_INIT_FAILED,
                "JobManager has been created already");
        this.jobManagerRunnerFuture.completeExceptionally(failureCause);
        status = Status.JOB_MANAGER_CREATED_OR_INIT_FAILED;
    }

    public void setCancelling() {
        Preconditions.checkState(status == Status.INITIALIZING, "JobManager must be initializing");
        status = Status.CANCELLING;
    }

    public void setInitializing() {
        Preconditions.checkState(
                status == Status.JOB_MANAGER_CREATED_OR_INIT_FAILED || status == Status.CANCELLING,
                "JobManager must be in state created or cancelling to be stopped");
        jobManagerRunnerFuture.completeExceptionally(
                new FlinkException("Initializing new JobManager."));
        jobManagerRunnerFuture = new CompletableFuture<>();
        status = Status.INITIALIZING;
    }

    public JobStatus asJobStatus() {
        if (status.getJobStatus() == null) {
            throw new IllegalStateException("This state is not defined as a 'JobStatus'");
        }
        return status.getJobStatus();
    }

    public boolean isInitializing() {
        return status == Status.INITIALIZING;
    }

    public boolean isJobManagerCreatedOrFailed() {
        return status == Status.JOB_MANAGER_CREATED_OR_INIT_FAILED;
    }

    public boolean isCancelling() {
        return status == Status.CANCELLING;
    }

    public CompletableFuture<JobManagerRunner> getJobManagerRunnerFuture() {
        Preconditions.checkState(
                !isInitializing(), "JobManagerRunner is not available during initialization");
        return jobManagerRunnerFuture;
    }

    private enum Status {
        // We are waiting for the JobManager to be created
        INITIALIZING(JobStatus.INITIALIZING),
        JOB_MANAGER_CREATED_OR_INIT_FAILED(null),
        // waiting for cancellation
        CANCELLING(JobStatus.CANCELLING);

        @Nullable private final JobStatus jobStatus;

        Status(JobStatus jobStatus) {
            this.jobStatus = jobStatus;
        }

        @Nullable
        public JobStatus getJobStatus() {
            return jobStatus;
        }
    }
}
