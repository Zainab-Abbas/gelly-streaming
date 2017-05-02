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


import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.generator.CompleteGraph;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.List;

public class filterEdges implements ProgramDescription {

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		long vertexCount = 4;

		Graph<LongValue, NullValue, NullValue> graph = new CompleteGraph(env, vertexCount)
															   .setParallelism(1)
															   .generate();



		graph.filterOnEdges(new FilterFunction<Edge<LongValue, NullValue>>() {
			@Override
			public boolean filter(Edge<LongValue, NullValue> edge) throws Exception {

					LongValue s= edge.getSource();
					LongValue t= edge.getTarget();

					if(s.compareTo(t)>0){
						return false;
					}
					return true;
			}
		}).getEdgeIds().writeAsCsv("/Users/zainababbas/working/gelly-streaming/2e", FileSystem.WriteMode.OVERWRITE);

		File file = new File("/Users/zainababbas/partitioning/gelly-streaming/");

		// if file doesnt exists, then create it
		if (!file.exists()) {
			file.createNewFile();
		}

		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		filter f = new filter();
		List<org.apache.flink.graph.streaming.partitioner.inputgenerator.Edge> dataset= f.filterd("/Users/zainababbas/working/gelly-streaming/graph/4M");
		//List<Tuple2> dataset = f.filterd("/Users/zainababbas/working/gelly-streaming/2r");
      for(int i=0; i<dataset.size();i++)
		{
			bw.write(dataset.get(i).toString());
			bw.write("\n");
		}
bw.close();
		env.execute("Streaming Connected Components");
	}

	@Override
	public String getDescription() {
		return null;
	}
}
