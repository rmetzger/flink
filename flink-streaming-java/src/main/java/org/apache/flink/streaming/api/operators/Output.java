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
package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

/**
 * A {@link org.apache.flink.streaming.api.operators.StreamOperator} is supplied with an object
 * of this interface that can be used to emit elements and other messages, such as barriers
 * and watermarks, from an operator.
 *
 * @param <T> The type of the elements that can be emitted.
 */
@Experimental
public interface Output<T> extends Collector<T> {

	/**
	 * Emits a {@link Watermark} from an operator. This watermark is broadcast to all downstream
	 * operators.
	 *
	 * <p>A watermark specifies that no element with a timestamp older or equal to the watermark
	 * timestamp will be emitted in the future.
	 */
	void emitWatermark(Watermark mark);
}
