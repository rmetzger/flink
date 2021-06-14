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

package org.apache.flink.runtime.rest.handler.logbundler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.runtime.blob.TransientBlobService;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.AbstractHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.util.HandlerUtils;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.logbundler.LogBundlerActionQueryParameter;
import org.apache.flink.runtime.rest.messages.logbundler.LogBundlerMessageParameters;
import org.apache.flink.runtime.rest.messages.logbundler.LogBundlerStatus;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.taskexecutor.FileType;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.utils.IOUtils;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.rest.handler.resourcemanager.AbstractResourceManagerHandler.getResourceManagerGateway;
import static org.apache.flink.util.Preconditions.checkState;

/** TODOs: - ui change - add tests - open PR :win: */
public class LogBundlerHandler
        extends AbstractHandler<RestfulGateway, EmptyRequestBody, LogBundlerMessageParameters> {

    private final Object statusLock = new Object();
    private final ScheduledExecutorService executor;
    private final File bundlerFile;
    private final File localLogDir;
    private final TransientBlobService transientBlobService;

    @GuardedBy("statusLock")
    private volatile Status status = Status.IDLE;

    private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;

    public enum Status {
        IDLE,
        PROCESSING,
        BUNDLE_READY,
        BUNDLE_FAILED
    }

    public LogBundlerHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<EmptyRequestBody, LogBundlerStatus, LogBundlerMessageParameters>
                    messageHeaders,
            ScheduledExecutorService executor,
            Configuration clusterConfiguration,
            @Nullable File localLogDir,
            GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
            TransientBlobService transientBlobService) {
        super(leaderRetriever, timeout, responseHeaders, messageHeaders);
        this.executor = executor;
        this.resourceManagerGatewayRetriever = resourceManagerGatewayRetriever;
        this.transientBlobService = transientBlobService;
        String[] tmpDirs = ConfigurationUtils.parseTempDirectories(clusterConfiguration);
        this.bundlerFile =
                new File(
                        tmpDirs[0]
                                + File.separator
                                + System.currentTimeMillis()
                                + "-flink-log-bundle.tgz");
        bundlerFile.deleteOnExit();

        this.localLogDir = localLogDir;
    }

    @Override
    protected CompletableFuture<Void> respondToRequest(
            ChannelHandlerContext ctx,
            HttpRequest httpRequest,
            HandlerRequest<EmptyRequestBody, LogBundlerMessageParameters> handlerRequest,
            RestfulGateway gateway)
            throws RestHandlerException {

        List<String> queryParams =
                handlerRequest.getQueryParameter(LogBundlerActionQueryParameter.class);
        if (!queryParams.isEmpty()) {
            synchronized (statusLock) {
                final String action = queryParams.get(0);
                if ("download".equals(action)) {
                    if (status != Status.BUNDLE_READY) {
                        throw new RestHandlerException(
                                "There is no bundle ready to be downloaded",
                                HttpResponseStatus.BAD_REQUEST);
                    }
                    try {
                        HandlerUtils.transferFile(ctx, bundlerFile, httpRequest);
                        return CompletableFuture.completedFuture(null);
                    } catch (FlinkException e) {
                        log.warn("Error while transferring file", e);
                    }
                } else if ("trigger".equals(action)) {
                    if (status == Status.PROCESSING) {
                        throw new RestHandlerException(
                                "Unable to trigger log bundling while in status "
                                        + Status.PROCESSING,
                                HttpResponseStatus.BAD_REQUEST);
                    }
                    status = Status.PROCESSING;
                    executor.execute(this::collectAndCompressLogs);
                } else {
                    log.warn("Unknown action passed: '{}'", action);
                }
            }
        }

        // access status outside of lock to respond to status queries while processing.
        return HandlerUtils.sendResponse(
                ctx,
                httpRequest,
                new LogBundlerStatus(status),
                HttpResponseStatus.OK,
                responseHeaders);
    }

    private void collectAndCompressLogs() {
        synchronized (statusLock) {
            try {
                checkState(status == Status.PROCESSING);

                try (OutputStream fo =
                                Files.newOutputStream(
                                        bundlerFile.toPath(),
                                        StandardOpenOption.CREATE,
                                        StandardOpenOption.WRITE,
                                        StandardOpenOption.TRUNCATE_EXISTING);
                        OutputStream gzo = new GzipCompressorOutputStream(fo);
                        ArchiveOutputStream archiveOutputStream = new TarArchiveOutputStream(gzo)) {
                    collectLocalLogs(archiveOutputStream);
                    collectTaskManagerLogs(archiveOutputStream);
                    archiveOutputStream.finish();
                }

                status = Status.BUNDLE_READY;
            } catch (Throwable throwable) {
                status = Status.BUNDLE_FAILED;
                log.warn(
                        "Error while collecting and compressing logs with the log bundler",
                        throwable);
            }
        }
    }

    private void collectTaskManagerLogs(ArchiveOutputStream archiveOutputStream)
            throws RestHandlerException, ExecutionException, InterruptedException {
        final Time timeout = Time.seconds(10);
        final ResourceManagerGateway resourceManagerGateway =
                getResourceManagerGateway(resourceManagerGatewayRetriever);
        Collection<TaskManagerInfo> taskManagers =
                resourceManagerGateway.requestTaskManagerInfo(timeout).get();
        Collection<CompletableFuture<Optional<TaskManagerLogAndId>>> taskManagerLogsFuture =
                new ArrayList<>(taskManagers.size());
        for (TaskManagerInfo taskManagerInfo : taskManagers) {
            taskManagerLogsFuture.add(
                    resourceManagerGateway
                            .requestTaskManagerFileUploadByType(
                                    taskManagerInfo.getResourceId(), FileType.LOG, timeout)
                            .thenApplyAsync(
                                    tmLogBlobKey -> {
                                        try {
                                            return Optional.of(
                                                    new TaskManagerLogAndId(
                                                            taskManagerInfo.getResourceId(),
                                                            transientBlobService.getFile(
                                                                    tmLogBlobKey)));
                                        } catch (IOException e) {
                                            log.warn(
                                                    "Error while retrieving log from TaskManager",
                                                    e);
                                            return Optional.empty();
                                        }
                                    },
                                    executor));
        }
        FutureUtils.combineAll(taskManagerLogsFuture)
                .thenAccept(
                        taskManagerLogFiles ->
                                taskManagerLogFiles.forEach(
                                        taskManagerLogFileOptional ->
                                                taskManagerLogFileOptional.ifPresent(
                                                        taskManagerLogFile ->
                                                                addTaskManagerLogFile(
                                                                        taskManagerLogFile,
                                                                        archiveOutputStream))))
                .get();
    }

    private void addTaskManagerLogFile(
            TaskManagerLogAndId logFile, ArchiveOutputStream outputStream) {
        try {
            addArchiveEntry(
                    "taskmanager-" + logFile.getTaskManagerId().toString() + ".log",
                    logFile.getLogFile(),
                    outputStream);
        } catch (IOException e) {
            log.warn("Error while adding TaskManager log file to archive", e);
        }
    }

    private void collectLocalLogs(ArchiveOutputStream archiveOutputStream) throws IOException {
        File[] localLogFiles = localLogDir.listFiles((dir, name) -> name.endsWith(".log"));
        if (localLogFiles == null || localLogFiles.length == 0) {
            return;
        }
        for (File localLogFile : localLogFiles) {
            addArchiveEntry(localLogFile.getName(), localLogFile, archiveOutputStream);
        }
    }

    private void addArchiveEntry(String entryName, File file, ArchiveOutputStream outputStream)
            throws IOException {
        // todo: dedupe entryNames
        ArchiveEntry entry = outputStream.createArchiveEntry(file, entryName);
        outputStream.putArchiveEntry(entry);
        if (file.isFile()) {
            try (InputStream inputStream = Files.newInputStream(file.toPath())) {
                IOUtils.copy(inputStream, outputStream);
            }
        }
        outputStream.closeArchiveEntry();
    }

    private static class TaskManagerLogAndId {
        private final ResourceID taskManagerId;
        private final File logFile;

        public TaskManagerLogAndId(ResourceID taskManagerId, File logFile) {
            this.taskManagerId = taskManagerId;
            this.logFile = logFile;
        }

        public ResourceID getTaskManagerId() {
            return taskManagerId;
        }

        public File getLogFile() {
            return logFile;
        }
    }
}
