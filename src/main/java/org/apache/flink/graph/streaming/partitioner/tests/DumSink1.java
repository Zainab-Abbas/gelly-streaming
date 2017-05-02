package org.apache.flink.graph.streaming.partitioner.tests;

import org.apache.flink.graph.streaming.summaries.Candidates;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Created by zainababbas on 29/03/2017.
 */
public class DumSink1 implements SinkFunction<Candidates> {

	private static final long serialVersionUID = 1876986644706201196L;


	public DumSink1() {
	}

	@Override
	public void invoke(Candidates candidates) throws Exception {

	}
}
