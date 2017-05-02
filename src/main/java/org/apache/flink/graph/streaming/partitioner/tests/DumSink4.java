package org.apache.flink.graph.streaming.partitioner.tests;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * Created by zainababbas on 03/04/2017.
 */
public class DumSink4 implements SinkFunction<Tuple2<Integer, Integer>> {



	@Override
	public void invoke(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {

	}
}
