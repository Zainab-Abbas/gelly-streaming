package org.apache.flink.graph.streaming.partitioner.tests;

import org.apache.flink.graph.streaming.summaries.DisjointSet;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Created by zainababbas on 29/03/2017.
 */
public class DumSink3 implements SinkFunction<DisjointSet<Long>> {

	private static final long serialVersionUID = 1876986644706201196L;


	public DumSink3() {

	}


	@Override
	public void invoke(DisjointSet<Long> longDisjointSet) throws Exception {

	}
}
