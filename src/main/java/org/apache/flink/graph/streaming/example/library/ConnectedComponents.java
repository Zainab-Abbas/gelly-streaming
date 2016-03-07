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

package org.apache.flink.graph.streaming.example.library;

import java.io.Serializable;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.graph.streaming.EdgesFold;
import org.apache.flink.graph.streaming.WindowGraphAggregation;
import org.apache.flink.graph.streaming.example.IterativeConnectedComponents;
import org.apache.flink.graph.streaming.example.util.DisjointSet;
import org.apache.flink.types.NullValue;

/**
 * The Connected Components algorithm assigns a component ID to each vertex in the graph.
 * Vertices that belong to the same component have the same component ID.
 * This algorithm computes _weakly_ connected components, i.e. edge direction is ignored.
 * <p>
 * This is a single-pass implementation, which uses a {@link WindowGraphAggregation} to periodically merge
 * the partitioned state. For an iterative implementation, see {@link IterativeConnectedComponents}.
 */
public class ConnectedComponents <K extends Serializable, EV> extends WindowGraphAggregation<K, EV, DisjointSet<K>, DisjointSet<K>> implements Serializable{

    private long mergeWindowTime;


    public ConnectedComponents(long mergeWindowTime) {
        super(new UpdateCC(),new CombineCC(), new DisjointSet<K>(), mergeWindowTime, false);
    }

    public static class CombineCC <K extends Serializable> implements ReduceFunction<DisjointSet<K>> {
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

    public final static class UpdateCC <K extends Serializable> implements EdgesFold<K, NullValue, DisjointSet<K>> {

        @Override
        public DisjointSet<K> foldEdges(DisjointSet<K> ds, K vertex, K vertex2, NullValue edgeValue) throws Exception {
            ds.union(vertex, vertex2);
            return ds;
        }
    }

}







