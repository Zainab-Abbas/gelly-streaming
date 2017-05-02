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

package org.apache.flink.graph.streaming.library;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.graph.streaming.EdgesFold;
import org.apache.flink.graph.streaming.WindowGraphAggregation;
import org.apache.flink.graph.streaming.example.IterativeConnectedComponents;
import org.apache.flink.graph.streaming.summaries.HMap;
import org.apache.flink.types.NullValue;

import java.io.Serializable;

/**
 * The Connected Components library method assigns a component ID to each vertex in the graph.
 * Vertices that belong to the same component have the same component ID.
 * This algorithm computes _weakly_ connected components, i.e. edge direction is ignored.
 * <p>
 * This is a single-pass implementation, which uses a {@link WindowGraphAggregation} to periodically merge
 * the partitioned state. For an iterative implementation, see {@link IterativeConnectedComponents}.
 *
 * @param <K>  the vertex ID type
 * @param <EV> the edge value type
 */
public class DegreeCheck<K extends Serializable, EV> extends WindowGraphAggregation<K, EV, HMap, HMap>implements Serializable {
	private static int mycount = 0;
	/**
	 * Creates a ConnectedComponents object using WindowGraphAggregation class.
	 * To find number of Connected Components the ConnectedComponents object is passed as an argument
	 * to the aggregate function of the {@link org.apache.flink.graph.streaming.GraphStream} class.
	 * Creating the ConnectedComponents object sets the EdgeFold, ReduceFunction, Initial Value,
	 * MergeWindow Time and Transient State for using the Window Graph Aggregation class.
	 *
	 * @param mergeWindowTime Window time in millisec for the merger.
	 */
	public DegreeCheck(long mergeWindowTime) {
		super(new UpdateCC(), new CombineCC(), new HMap(), mergeWindowTime, false);
	}

	/**
	 * Implements EdgesFold Interface, applies foldEdges function to
	 * a vertex neighborhood
	 * The Edge stream is divided into different windows, the foldEdges function
	 * is applied on each window incrementally and the aggregate state for each window
	 * is updated, in this case it checks the connected components in a window. If
	 * there is an edge between two vertices then they become part of a connected component.
	 *
	 * @param <K> the vertex ID type
	 */
	public final static class UpdateCC<K extends Serializable> implements EdgesFold<Long, NullValue, HMap> {


		private  static int count = 0;



		@Override
		public HMap foldEdges(HMap verticesWithDegrees, Long vertex, Long vertex2, NullValue edgeValue) throws Exception {
			verticesWithDegrees.union(vertex, vertex2);
			return verticesWithDegrees;
		}

	}

	/**
	 * Implements the ReduceFunction Interface, applies reduce function to
	 * combine group of elements into a single value.
	 * The aggregated states from different windows are combined together
	 * and reduced to a single result.
	 * In this case the values of the vertices belonging to Connected Components form
	 * each window are merged to find the Connected Components for the whole graph.
	 */

	public static class CombineCC<K extends Serializable> implements ReduceFunction<HMap> {



		@Override
		public HMap reduce(HMap s1, HMap s2) throws Exception {

			mycount++;


			System.out.println(mycount+"mycount");

			int count1 = s1.size();
			int count2 = s2.size();

			if (count1 <= count2) {
				s2.merge(s1.getmap());
				return s2;
			}
			s1.merge(s2.getmap());
			return s1;
		}
	}
}







