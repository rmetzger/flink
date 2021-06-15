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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rest.handler.taskmanager.AbstractTaskManagerFileHandlerTest;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/** Test for the {@link LogBundlerHandler}. */
public class LogBundlerHandlerTest extends TestLogger {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	private static final DefaultFullHttpRequest HTTP_REQUEST =
		new DefaultFullHttpRequest(
			HttpVersion.HTTP_1_1, HttpMethod.GET, AbstractTaskManagerFileHandlerTest.TestUntypedMessageHeaders.URL);

	@Test
    public void testNoTmpDir() throws IOException {
        Configuration config = new Configuration();
        LogBundlerHandler handler = createHandler(config);

		final AbstractTaskManagerFileHandlerTest.TestingChannelHandlerContext testingContext =
			new AbstractTaskManagerFileHandlerTest.TestingChannelHandlerContext(tempFolder.newFile());
        handler.respondToRequest(testingContext, HTTP_REQUEST)
    }

	private LogBundlerHandler createHandler(Configuration config) {
		return null;
	}
}
