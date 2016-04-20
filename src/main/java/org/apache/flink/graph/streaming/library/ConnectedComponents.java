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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.graph.streaming.EdgesFold;
import org.apache.flink.graph.streaming.GraphWindowStream;
import org.apache.flink.graph.streaming.WindowGraphAggregation;
import org.apache.flink.graph.streaming.example.IterativeConnectedComponents;
import org.apache.flink.graph.streaming.example.util.DisjointSet;
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
public class ConnectedComponents<K extends Serializable, EV> extends WindowGraphAggregation<K, EV, DisjointSet<K>, DisjointSet<K>> implements Serializable {

	private long mergeWindowTime;

	/**
	 * Creates a ConnectedComponents object using WindowGraphAggregation class.
	 * This helps dividing the edge stream into parallel window for each partition.
	 *
	 * @param mergeWindowTime Window time in millisec for the merger.
	 */
	public ConnectedComponents(long mergeWindowTime) {
		super(new UpdateCC(), new CombineCC(), new DisjointSet<K>(), mergeWindowTime, false);
	}

	/**
	 * Implements EdgesFold Interface, applies foldEdges function to
	 * a vertex neighborhood
	 *
	 * @param <K> the vertex ID type
	 */
	public final static class UpdateCC<K extends Serializable> implements EdgesFold<K, NullValue, DisjointSet<K>> {

		/**
		 * Implements foldEdges method of EdgesFold interface for combining
		 * two edges values into same type using union method of the DisjointSet class.
		 * In this case it updates the Connected Component value in each partition.
		 *
		 * @param ds        the initial value and accumulator
		 * @param vertex    the vertex ID
		 * @param vertex2   the neighbor's ID
		 * @param edgeValue the edge value
		 * @return The data stream that is the result of applying the foldEdges function to the graph window.
		 * @throws Exception
		 */
		@Override
		public DisjointSet<K> foldEdges(DisjointSet<K> ds, K vertex, K vertex2, NullValue edgeValue) throws Exception {
			ds.union(vertex, vertex2);
			return ds;
		}
	}

	/**
	 * Implements the ReduceFunction Interface, applies reduce function to
	 * combine group of elements into a single value.
	 */
	public static class CombineCC<K extends Serializable> implements ReduceFunction<DisjointSet<K>> {

		/**
		 * Implements reduce method of ReduceFunction interface.
		 * Two values of DisjointSet class are combined into one using merge method
		 * of the DisjointSet class.
		 * In this case the merge method takes Connected Components values from different
		 * partitions and merges them into one.
		 *
		 * @param s1 The first value to combine.
		 * @param s2 The second value to combine.
		 * @return The combined value of both input values.
		 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
		 *                   to fail and may trigger recovery.
		 */
		@Override
		public DisjointSet<K> reduce(DisjointSet<K> s1, DisjointSet<K> s2) throws Exception {
			int count1 = s1.getMatches().size();
			int count2 = s2.getMatches().size();
			if (count1 <= count2) {
				s2.merge(s1);
				return s2;
			}
			s1.merge(s2);
			return s1;
		}
	}
}







