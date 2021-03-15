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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.checkpoint.CheckpointScheduling;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * When a "stop with savepoint" operation (wait until savepoint has been created, then cancel job)
 * is triggered on the {@link Executing} state, we transition into this state.
 */
class StopWithSavepoint extends StateWithExecutionGraph {

    private final Context context;
    private final ClassLoader userCodeClassLoader;

    private final CompletableFuture<String> operationFuture;

    private final CheckpointScheduling checkpointScheduling;

    private boolean hasFullyFinished = false;

    @Nullable private String savepoint = null;

    StopWithSavepoint(
            Context context,
            ExecutionGraph executionGraph,
            ExecutionGraphHandler executionGraphHandler,
            OperatorCoordinatorHandler operatorCoordinatorHandler,
            CheckpointScheduling checkpointScheduling,
            Logger logger,
            ClassLoader userCodeClassLoader,
            CompletableFuture<String> savepointFuture) {
        super(context, executionGraph, executionGraphHandler, operatorCoordinatorHandler, logger);
        this.context = context;
        this.userCodeClassLoader = userCodeClassLoader;
        this.checkpointScheduling = checkpointScheduling;
        this.operationFuture = new CompletableFuture<>();

        FutureUtils.assertNoException(
                savepointFuture.handle(
                        (savepointLocation, throwable) -> {
                            context.runIfState(
                                    this,
                                    () -> handleSavepointCompletion(savepointLocation, throwable),
                                    Duration.ZERO);
                            return null;
                        }));
    }

    private void handleSavepointCompletion(
            @Nullable String savepoint, @Nullable Throwable throwable) {

        if (hasFullyFinished) {
            if (throwable != null) {
                throw new IllegalStateException(
                        "A savepoint should never fail after a job has been terminated via stop-with-savepoint.");
            } else {
                completeOperationAndGoToFinished(savepoint);
            }
        } else {
            if (throwable != null) {
                getLogger()
                        .warn(
                                "Continuing execution because creating the savepoint failed with",
                                throwable);
                checkpointScheduling.startCheckpointScheduler();
                context.goToExecuting(
                        getExecutionGraph(),
                        getExecutionGraphHandler(),
                        getOperatorCoordinatorHandler());
            } else {
                this.savepoint = savepoint;
            }
        }
    }

    @Override
    public void onLeave(Class<? extends State> newState) {
        this.operationFuture.completeExceptionally(
                new FlinkException("Stop with savepoint operation could not be completed."));

        super.onLeave(newState);
    }

    @Override
    public void cancel() {
        context.goToCanceling(
                getExecutionGraph(), getExecutionGraphHandler(), getOperatorCoordinatorHandler());
    }

    @Override
    public JobStatus getJobStatus() {
        return JobStatus.RUNNING;
    }

    @Override
    public void handleGlobalFailure(Throwable cause) {
        handleAnyFailure(cause);
    }

    @Override
    boolean updateTaskExecutionState(TaskExecutionStateTransition taskExecutionStateTransition) {
        final boolean successfulUpdate =
                getExecutionGraph().updateState(taskExecutionStateTransition);

        if (successfulUpdate) {
            if (taskExecutionStateTransition.getExecutionState() == ExecutionState.FAILED) {
                Throwable cause = taskExecutionStateTransition.getError(userCodeClassLoader);
                handleAnyFailure(cause);
            }
        }

        return successfulUpdate;
    }

    @Override
    void onGloballyTerminalState(JobStatus globallyTerminalState) {
        if (globallyTerminalState == JobStatus.FINISHED) {
            if (savepoint == null) {
                hasFullyFinished = true;
            } else {
                completeOperationAndGoToFinished(savepoint);
            }
        } else {
            handleAnyFailure(new FlinkException("Job did not finish properly."));
        }
    }

    private void completeOperationAndGoToFinished(String savepoint) {
        operationFuture.complete(savepoint);
        context.goToFinished(ArchivedExecutionGraph.createFrom(getExecutionGraph()));
    }

    private void handleAnyFailure(Throwable cause) {
        final Executing.FailureResult failureResult = context.howToHandleFailure(cause);

        if (failureResult.canRestart()) {
            context.goToRestarting(
                    getExecutionGraph(),
                    getExecutionGraphHandler(),
                    getOperatorCoordinatorHandler(),
                    failureResult.getBackoffTime());
        } else {
            context.goToFailing(
                    getExecutionGraph(),
                    getExecutionGraphHandler(),
                    getOperatorCoordinatorHandler(),
                    failureResult.getFailureCause());
        }
    }

    CompletableFuture<String> getOperationFuture() {
        return operationFuture;
    }

    interface Context extends StateWithExecutionGraph.Context {
        /**
         * Asks how to handle the failure.
         *
         * @param failure failure describing the failure cause
         * @return {@link Executing.FailureResult} which describes how to handle the failure
         */
        Executing.FailureResult howToHandleFailure(Throwable failure);

        /**
         * Transitions into the {@link Canceling} state.
         *
         * @param executionGraph executionGraph to pass to the {@link Canceling} state
         * @param executionGraphHandler executionGraphHandler to pass to the {@link Canceling} state
         * @param operatorCoordinatorHandler operatorCoordinatorHandler to pass to the {@link
         *     Canceling} state
         */
        void goToCanceling(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler);

        /**
         * Transitions into the {@link Restarting} state.
         *
         * @param executionGraph executionGraph to pass to the {@link Restarting} state
         * @param executionGraphHandler executionGraphHandler to pass to the {@link Restarting}
         *     state
         * @param operatorCoordinatorHandler operatorCoordinatorHandler to pas to the {@link
         *     Restarting} state
         * @param backoffTime backoffTime to wait before transitioning to the {@link Restarting}
         *     state
         */
        void goToRestarting(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Duration backoffTime);

        /**
         * Transitions into the {@link Failing} state.
         *
         * @param executionGraph executionGraph to pass to the {@link Failing} state
         * @param executionGraphHandler executionGraphHandler to pass to the {@link Failing} state
         * @param operatorCoordinatorHandler operatorCoordinatorHandler to pass to the {@link
         *     Failing} state
         * @param failureCause failureCause describing why the job execution failed
         */
        void goToFailing(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Throwable failureCause);

        /**
         * Transitions into the {@link Executing} state.
         *
         * @param executionGraph executionGraph to pass to the {@link Executing} state
         * @param executionGraphHandler executionGraphHandler to pass to the {@link Executing} state
         * @param operatorCoordinatorHandler operatorCoordinatorHandler to pass to the {@link
         *     Executing} state
         */
        void goToExecuting(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler);

        /**
         * Runs the given action after the specified delay if the state is the expected state at
         * this time.
         *
         * @param expectedState expectedState describes the required state to run the action after
         *     the delay
         * @param action action to run if the state equals the expected state
         * @param delay delay after which the action should be executed
         */
        void runIfState(State expectedState, Runnable action, Duration delay);
    }

    static class Factory implements StateFactory<StopWithSavepoint> {
        private final Context context;

        private final ExecutionGraph executionGraph;

        private final ExecutionGraphHandler executionGraphHandler;

        private final OperatorCoordinatorHandler operatorCoordinatorHandler;

        private final CheckpointScheduling checkpointScheduling;

        private final Logger logger;

        private final ClassLoader userCodeClassLoader;

        private final CompletableFuture<String> savepointFuture;

        Factory(
                Context context,
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                CheckpointScheduling checkpointScheduling,
                Logger logger,
                ClassLoader userCodeClassLoader,
                CompletableFuture<String> savepointFuture) {
            this.context = context;
            this.executionGraph = executionGraph;
            this.executionGraphHandler = executionGraphHandler;
            this.operatorCoordinatorHandler = operatorCoordinatorHandler;
            this.checkpointScheduling = checkpointScheduling;
            this.logger = logger;
            this.userCodeClassLoader = userCodeClassLoader;
            this.savepointFuture = savepointFuture;
        }

        @Override
        public Class<StopWithSavepoint> getStateClass() {
            return StopWithSavepoint.class;
        }

        @Override
        public StopWithSavepoint getState() {
            return new StopWithSavepoint(
                    context,
                    executionGraph,
                    executionGraphHandler,
                    operatorCoordinatorHandler,
                    checkpointScheduling,
                    logger,
                    userCodeClassLoader,
                    savepointFuture);
        }
    }
}
