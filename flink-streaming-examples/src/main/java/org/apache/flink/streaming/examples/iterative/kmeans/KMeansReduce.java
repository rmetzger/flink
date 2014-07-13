/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.streaming.examples.iterative.kmeans;

import org.apache.flink.streaming.api.invokable.UserTaskInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;

import org.apache.flink.api.java.tuple.Tuple1;

//public class KMeansReduce extends UserTaskInvokable {
//
//	private static final long serialVersionUID = 1L;
//	private StreamRecord outRecord = new StreamRecord(new Tuple1<String>());
//	private double[] point=null;
//	public KMeansReduce(int dimension){
//		point = new double[dimension];
//	}
//	
//	@Override
//	public void invoke(StreamRecord record) throws Exception {
//		String[] pointStr = record.getString(0, 0).split(" ");
//		for(int i=0; i<pointStr.length; ++i){
//			point[i]=Double.valueOf(pointStr[i]);
//		}
//		outRecord.setString(0, record.getString(0, 0));
//		emit(outRecord);
//	}
//
//}
