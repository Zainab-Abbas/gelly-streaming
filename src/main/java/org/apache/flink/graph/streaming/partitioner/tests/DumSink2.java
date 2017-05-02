package org.apache.flink.graph.streaming.partitioner.tests;

import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.NullValue;

/**
 * Created by zainababbas on 29/03/2017.
 */
public class DumSink2 implements SinkFunction<Edge<Long, NullValue>> {

	private static final long serialVersionUID = 1876986644706201196L;


	public DumSink2() {

	}



	@Override
	public void invoke(Edge<Long, NullValue> longNullValueEdge) throws Exception {

	}
}
