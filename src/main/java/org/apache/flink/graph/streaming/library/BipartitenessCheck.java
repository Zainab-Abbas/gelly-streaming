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
import org.apache.flink.graph.streaming.example.util.Candidates;
import org.apache.flink.graph.streaming.example.util.SignedVertex;
import org.apache.flink.types.NullValue;

import java.io.Serializable;

/**
 * The bipartiteness check library to check whether an input graph is bipartite
 * or not. A bipartite graph's vertices can be separated into two disjoint
 * groups, such as no two nodes inside the same group is connected by an edge.
 * The library uses the merge-tree abstraction of our graph streaming API.
 *
 * @param <K>  the vertex ID type
 * @param <EV> the edge value type
 */

public class BipartitenessCheck<K extends Serializable, EV> extends WindowGraphAggregation<K, EV, Candidates, Candidates> implements Serializable {

    public BipartitenessCheck() {
        super(new updateFunction(), new combineFunction(), new Candidates(true), 500, false);
    }

    @SuppressWarnings("serial")

    /**
     * Implements EdgesFold Interface, applies a fuction to a vertex neighborhood
     * in the {@link GraphWindowStream#foldNeighbors(Object, EdgesFold)} method.
     *
     * @param <K> the vertex ID type
     */
    public static class updateFunction<K extends Serializable> implements EdgesFold<Long, NullValue, Candidates> {
        /**
         * Combines two edge values into one value of the same type.
         * The foldEdges function is consecutively applied to all edges of a neighborhood,
         * until only a single value remains.
         *
         * @param candidates the initial value and accumulator
         * @param v1         the vertex ID
         * @param v2         the neighbor's ID
         * @param edgeVal    the edge value
         * @return The data stream that is the result of applying the foldEdges function to the graph window.
         * @throws Exception
         */
        @Override
        public Candidates foldEdges(Candidates candidates, Long v1, Long v2, NullValue edgeVal) throws Exception {
            return candidates.merge(edgeToCandidate(v1, v2));
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
    public static class combineFunction implements ReduceFunction<Candidates> {
        /**
         * The core method of ReduceFunction, combining two values into one value of the same type.
         * The reduce function is consecutively applied to all values of a group until only a single value remains.
         *
         * @param c1 The first value to combine.
         * @param c2 The second value to combine.
         * @return The combined value of both input values.
         * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
         *                   to fail and may trigger recovery.
         */
        @Override
        public Candidates reduce(Candidates c1, Candidates c2) throws Exception {
            return c1.merge(c2);
        }
    }

    public static Candidates edgeToCandidate(long v1, long v2) throws Exception {
        long src = Math.min(v1, v2);
        long trg = Math.max(v1, v2);
        Candidates cand = new Candidates(true);
        cand.add(src, new SignedVertex(src, true));
        cand.add(src, new SignedVertex(trg, false));
        return cand;
    }

}
