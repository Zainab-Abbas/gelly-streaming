package org.apache.flink.graph.streaming.partitioner.tests;

import org.apache.flink.graph.streaming.summaries.AdjacencyListGraph;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Created by zainababbas on 29/03/2017.
 */
public class DumSink  implements SinkFunction<AdjacencyListGraph<Long>> {

	private static final long serialVersionUID = 1876986644706201196L;


	private String output;

	public DumSink() {
	}

	@Override
	public void invoke(AdjacencyListGraph<Long> longAdjacencyListGraph) throws Exception {

	}
}
