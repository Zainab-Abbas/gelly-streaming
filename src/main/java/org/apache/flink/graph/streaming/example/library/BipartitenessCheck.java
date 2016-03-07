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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.graph.streaming.EdgesFold;
import org.apache.flink.graph.streaming.WindowGraphAggregation;
import org.apache.flink.graph.streaming.example.util.Candidates;
import org.apache.flink.graph.streaming.example.util.SignedVertex;
import org.apache.flink.types.NullValue;

import java.io.Serializable;

/**
 * The bipartiteness check example tests whether an input graph is bipartite
 * or not. A bipartite graph's vertices can be separated into two disjoint
 * groups, such as no two nodes inside the same group is connected by an edge.
 * The example uses the merge-tree abstraction of our graph streaming API.
 */
public class BipartitenessCheck <K extends Serializable,EV> extends WindowGraphAggregation<K,EV,Candidates, Candidates> implements Serializable {

    public BipartitenessCheck() {
        super( new updFun(), new combineFun (), new Candidates(true), 500, false);
    }

    @SuppressWarnings("serial")

    public static class updFun <K extends Serializable> implements EdgesFold<Long, NullValue, Candidates> {

        @Override
        public Candidates foldEdges(Candidates candidates, Long v1, Long v2, NullValue edgeVal) throws Exception {
            return candidates.merge(edgeToCandidate(v1, v2));
        }
    };

    public static class combineFun implements ReduceFunction<Candidates> {
        @Override
        public Candidates reduce(Candidates c1, Candidates c2) throws Exception {
            return c1.merge(c2);
        }

    };


    public static Candidates edgeToCandidate (long v1,long v2) throws Exception {
        long src = Math.min(v1, v2);
        long trg = Math.max(v1, v2);
        Candidates cand = new Candidates(true);
        cand.add(src, new SignedVertex(src, true));
        cand.add(src, new SignedVertex(trg, false));
        return cand;
    }

}
