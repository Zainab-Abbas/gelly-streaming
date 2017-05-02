package org.apache.flink.graph.streaming.partitioner.tests;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * Created by zainababbas on 20/02/2017.
 */
public class CutSink implements SinkFunction<Tuple2<Long, List<Long>>> {

	private static final long serialVersionUID = 1876986644706201196L;


	private String output;

	public CutSink(String outputPath) {
		this.output=outputPath;
	}

	@Override
	public void invoke(Tuple2<Long, List<Long>> value) {

		try {
			FileWriter fw = new FileWriter(output, true); //the true will append the new data

			fw.write(value.f0 + "\n");
			fw.close();
		} catch (IOException ioe) {
			System.err.println("IOException: " + ioe.getMessage());
		}
	}


}