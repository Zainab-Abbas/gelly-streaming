package org.apache.flink.graph.streaming.partitioner.tests;

import org.apache.flink.graph.streaming.summaries.HMap;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Created by zainababbas on 29/03/2017.
 */
public class DumSink5 implements SinkFunction<HMap> {

	private static final long serialVersionUID = 1876986644706201196L;


	public DumSink5() {
	}

	@Override
	public void invoke(HMap map) throws Exception {

	}
}
