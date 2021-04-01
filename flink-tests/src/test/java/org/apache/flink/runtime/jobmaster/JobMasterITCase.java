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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.apache.flink.core.testutils.FlinkMatchers.containsMessage;
import static org.junit.Assert.assertThat;

public class JobMasterITCase extends TestLogger {
    private static final String FAILURE_MESSAGE = "Intentional Test failure";

    /**
     * This test is to guard against FLINK-22001, where any exception from the JobManager
     * initialization was not forwarded to the user.
     */
    @Test
    public void testJobManagerInitializationExceptionsAreForwardedToTheUser()
            throws InterruptedException {
        // we must use the LocalStreamEnvironment to reproduce this issue.
        // It passes with the TestStreamEnvironment (which is initialized by the
        // MiniClusterResource). The LocalStreamEnvironment is polling the JobManager for the job
        // status, while TestStreamEnvironment is waiting on the resultFuture.
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        Source<String, MockSplit, Void> mySource = new FailOnInitializationSource();
        DataStream<String> stream =
                see.fromSource(mySource, WatermarkStrategy.noWatermarks(), "MySourceName");
        stream.addSink(new DiscardingSink<>());

        try {
            see.execute();
        } catch (Exception e) {
            log.info("caught", e);
            assertThat(e, containsMessage(FAILURE_MESSAGE));
        }
    }

    private static class FailOnInitializationSource implements Source<String, MockSplit, Void> {
        @Override
        public Boundedness getBoundedness() {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }

        @Override
        public SourceReader<String, MockSplit> createReader(SourceReaderContext readerContext)
                throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public SplitEnumerator<MockSplit, Void> createEnumerator(
                SplitEnumeratorContext<MockSplit> enumContext) throws Exception {
            // here, we fail in the JobMaster
            throw new RuntimeException("Intentional Test failure");
        }

        @Override
        public SplitEnumerator<MockSplit, Void> restoreEnumerator(
                SplitEnumeratorContext<MockSplit> enumContext, Void checkpoint) throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public SimpleVersionedSerializer<MockSplit> getSplitSerializer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
            throw new UnsupportedOperationException();
        }
    }

    private static class MockSplit implements SourceSplit {
        @Override
        public String splitId() {
            throw new UnsupportedOperationException();
        }
    }
}
