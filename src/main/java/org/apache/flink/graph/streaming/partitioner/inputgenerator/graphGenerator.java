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

package org.apache.flink.graph.streaming.partitioner.inputgenerator;


import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.generator.RMatGraph;
import org.apache.flink.graph.generator.random.JDKRandomGeneratorFactory;
import org.apache.flink.graph.generator.random.RandomGenerableFactory;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;


public class graphGenerator implements ProgramDescription {

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		RandomGenerableFactory<JDKRandomGenerator> rnd = new JDKRandomGeneratorFactory();

		//short scale =11;
		//int vertexCount = 2 << scale;
		int vertexCount =1750000;
		int edgeFactor =20;
		int edgeCount =vertexCount * edgeFactor;
		System.out.println(edgeCount);
		boolean clipAndFlip = false;

		Graph<LongValue,NullValue,NullValue> graph = new RMatGraph<>(env, rnd, vertexCount, edgeCount)
															 .setConstants(0.57f, 0.19f, 0.19f)
															 .setNoise(true, 0.10f)

		.setParallelism(1)
															 .generate();


		graph.filterOnEdges(new FilterFunction<Edge<LongValue, NullValue>>() {
			@Override
			public boolean filter(Edge<LongValue, NullValue> edge) throws Exception {

				LongValue s= edge.getSource();
				LongValue t= edge.getTarget();


				if(s.compareTo(t)==0 ){
					return false;
				}
				return true;
			}
		}).getEdgeIds().writeAsCsv("/Users/zainababbas/working/gelly-streaming/graph/4M", FileSystem.WriteMode.OVERWRITE);

		env.execute("Streaming Connected Components");
	}

	@Override
	public String getDescription() {
		return null;
	}
}

