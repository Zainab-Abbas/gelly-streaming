package org.apache.flink.graph.streaming.partitioner.tests;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * Created by zainababbas on 20/02/2017.
 */
public class TimestampingSinkv implements SinkFunction<Tuple3<Long, List<Long>,Long>> {

private static final long serialVersionUID = 1876986644706201196L;

private double maxLatency;
private long count;
private double minLatency=Long.MAX_VALUE;
private long sumLatency= 0;
private long avgLatency= 0;
private long total;
private String output;

public TimestampingSinkv(String outputPath) {
			   this.output=outputPath;
			   }

@Override
public void invoke(Tuple3<Long, List<Long>,Long> value) {

											count++;

											if (count % 200000 == 0) {
										/*	long ts = value.f2;
											if (ts != 0L) {
											total++;
											long diff = System.nanoTime() - ts;
											sumLatency = sumLatency + diff;
											maxLatency = Math.max(diff, maxLatency);
											minLatency = Math.min(diff, minLatency);
											avgLatency =  sumLatency/total;
				/*System.out.println("max latency: " + maxLatency);
				System.out.println("min latency: " + minLatency);
				System.out.println("total latency: " + total);
				System.out.println("avg latency: " + sumLatency/total);*/
											System.out.print(System.currentTimeMillis()-value.f2);
												System.out.print("time");
											try {
											FileWriter fw = new FileWriter(output, true); //the true will append the new data
										//	fw.write("max latency in millisec: " + maxLatency + "\n");//appends the string to the file
										//	fw.write("min latency in millisec: " + minLatency + "\n");
											fw.write(System.currentTimeMillis()-value.f2+ "\n");
											fw.close();
											} catch (IOException ioe) {
											System.err.println("IOException: " + ioe.getMessage());
											}

											count = 0;
											}
											}


}