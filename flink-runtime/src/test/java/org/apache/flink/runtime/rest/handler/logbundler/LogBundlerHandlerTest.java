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
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.taskmanager.AbstractTaskManagerFileHandlerTest;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.logbundler.LogBundlerHeaders;
import org.apache.flink.runtime.rest.messages.logbundler.LogBundlerMessageParameters;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/** Test for the {@link LogBundlerHandler}. */
public class LogBundlerHandlerTest extends TestLogger {

    private static final Time TEST_TIMEOUT = Time.seconds(10);
    private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(2);

    @Rule public static TemporaryFolder tempFolder = new TemporaryFolder();

    private static final DefaultFullHttpRequest HTTP_REQUEST =
            new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1,
                    HttpMethod.GET,
                    AbstractTaskManagerFileHandlerTest.TestUntypedMessageHeaders.URL);
    private static BlobServer blobServer;

    @BeforeClass
    public static void prepare() throws IOException {
        Configuration blobServerConfig = new Configuration();
        blobServerConfig.setString(
                BlobServerOptions.STORAGE_DIRECTORY, tempFolder.newFolder().getAbsolutePath());

        blobServer = new BlobServer(blobServerConfig, new VoidBlobStore());
    }

    @Test // (expected = RestHandlerException.class)
    public void testNoTmpDir() throws Exception {
        Configuration config = new Configuration();
        config.set(CoreOptions.TMP_DIRS, "");
        LogBundlerHandler handler = createHandler(config);

        File tmpFile = tempFolder.newFile();
        final AbstractTaskManagerFileHandlerTest.TestingChannelHandlerContext testingContext =
                new AbstractTaskManagerFileHandlerTest.TestingChannelHandlerContext(tmpFile);
        HandlerRequest<EmptyRequestBody, LogBundlerMessageParameters> handlerRequest =
                new HandlerRequest<>(
                        EmptyRequestBody.getInstance(), new LogBundlerMessageParameters());

        handler.respondToRequest(testingContext, HTTP_REQUEST, handlerRequest, null);

        log.info("res = " + new String(Files.readAllBytes(tmpFile.toPath())));
    }

    private LogBundlerHandler createHandler(Configuration config) throws IOException {
        return new LogBundlerHandler(
                () -> CompletableFuture.completedFuture(null),
                TEST_TIMEOUT,
                Collections.emptyMap(),
                LogBundlerHeaders.getInstance(),
                executor,
                config,
                null,
                () -> CompletableFuture.completedFuture(null),
                blobServer);
    }
}
