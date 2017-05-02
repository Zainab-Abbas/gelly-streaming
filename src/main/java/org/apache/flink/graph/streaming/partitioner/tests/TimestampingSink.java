package org.apache.flink.graph.streaming.partitioner.tests;

import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.NullValue;

/**
 * Created by zainababbas on 16/02/2017.
 */
public class TimestampingSink implements SinkFunction<Edge<Long, NullValue>> {

	private static final long serialVersionUID = 1876986644706201196L;

	//private double maxLatency;
	private long count;
	//private double minLatency=Long.MAX_VALUE;
	private double sumLatency= 0.0;
	private double avgLatency= 0.0;
	private long total;
	private String output;

	public TimestampingSink(String outputPath) {
		this.output=outputPath;
	}

	@Override
	public void invoke(Edge<Long, NullValue> value) {

		count++;

		if (count % 15551249 == 0) {


					System.err.println("lastoneklafkdakflaöklöflakfklaflk ");
				}


	}


}
