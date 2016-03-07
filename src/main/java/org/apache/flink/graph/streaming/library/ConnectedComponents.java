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
 * The Connected Components algorithm assigns a component ID to each vertex in the graph.
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

    public ConnectedComponents(long mergeWindowTime) {
        super(new UpdateCC(), new CombineCC(), new DisjointSet<K>(), mergeWindowTime, false);
    }

    /**
     * Implements EdgesFold Interface, applies a fuction to a vertex neighborhood
     * in the {@link GraphWindowStream#foldNeighbors(Object, EdgesFold)} method.
     *
     * @param <K> the vertex ID type
     */
    public final static class UpdateCC<K extends Serializable> implements EdgesFold<K, NullValue, DisjointSet<K>> {
        /**
         * Combines two edge values into one value of the same type.
         * The foldEdges function is consecutively applied to all edges of a neighborhood,
         * until only a single value remains.
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
     * Reduce functions combine groups of elements to
     * a single value, by taking always two elements and combining them into one. Reduce functions
     * may be used on entire data sets, or on grouped data sets. In the latter case, each group is reduced
     * individually.
     * <p>
     * For a reduce functions that work on an entire group at the same time (such as the
     * MapReduce/Hadoop-style reduce), see {@link GroupReduceFunction}. In the general case,
     * ReduceFunctions are considered faster, because they allow the system to use more efficient
     * execution strategies.
     * <p>
     * The basic syntax for using a grouped ReduceFunction is as follows:
     * <pre>{@code
     * DataSet<X> input = ...;
     *
     * DataSet<X> result = input.groupBy(<key-definition>).reduce(new MyReduceFunction());
     * }</pre>
     * <p>
     * Like all functions, the ReduceFunction needs to be serializable, as defined in {@link java.io.Serializable}.
     */
    public static class CombineCC<K extends Serializable> implements ReduceFunction<DisjointSet<K>> {
        /**
         * The core method of ReduceFunction, combining two values into one value of the same type.
         * The reduce function is consecutively applied to all values of a group until only a single value remains.
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







