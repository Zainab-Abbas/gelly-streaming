package org.apache.flink.graph.streaming.partitioner.edgepartitioners.batchapp;

/**
 * Created by zainababbas on 18/04/2017.
 */

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.Neighbor;
import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.graph.streaming.partitioner.edgepartitioners.keyselector.CustomKeySelector2;
import org.apache.flink.types.NullValue;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class GSASSSPHash implements ProgramDescription {

	// --------------------------------------------------------------------------------------------
	//  Program
	// --------------------------------------------------------------------------------------------

	public static void main(String[] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();



		DataSet<Edge<Long, NullValue>> data = env.readTextFile(edgesInputPath).map(new MapFunction<String, Edge<Long, NullValue>>() {

			@Override
			public Edge<Long, NullValue> map(String s) {
				String[] fields = s.split("\\,");
				long src = Long.parseLong(fields[0]);
				long trg = Long.parseLong(fields[1]);
				return new Edge<>(src, trg, NullValue.getInstance());
			}
		});

		env.setParallelism(k);
		//DataSet<Edge<Long, NullValue>> partitionedData =
			//		data.partitionCustom(new HashPartitioner<>(new CustomKeySelector(0)), new CustomKeySelector<>(0));

		Graph<Long, Double, NullValue> graph = Graph.fromDataSet(data.partitionCustom(new HashPartitioner<>(new CustomKeySelector2(0)), new CustomKeySelector2<>(0)), new InitVertices(srcVertexId), env);

		// Execute the GSA iteration
		Graph<Long, Double, NullValue> result = graph.runGatherSumApplyIteration(
				new CalculateDistances(), new ChooseMinDistance(), new UpdateDistance(), maxIterations);

		// Extract the vertices as the result
		DataSet<Vertex<Long, Double>> singleSourceShortestPaths = result.getVertices();


		// emit result
		if(fileOutput) {
			//singleSourceShortestPaths.writeAsCsv(outputPath, "\n", ",");
			singleSourceShortestPaths.writeAsCsv(outputPath, "\n", ",");
			// since file sinks are lazy, we trigger the execution explicitly
		} else {
			singleSourceShortestPaths.print();
		}

		JobExecutionResult result1 = env.execute("My Flink Job1");

		try {
			FileWriter fw = new FileWriter(logPath, true); //the true will append the new data
			//fw.write("The job took " + result.getNetRuntime(TimeUnit.SECONDS) + " seconds to execute" + "\n");//appends the string to the file
			//fw.write("The job took " + result.getNetRuntime(TimeUnit.NANOSECONDS) + " nanoseconds to execute" + "\n");
			fw.write("The job1 took " + result1.getNetRuntime(TimeUnit.SECONDS) + " seconds to execute" + "\n");//appends the string to the file
			fw.write("The job1 took " + result1.getNetRuntime(TimeUnit.NANOSECONDS) + " nanoseconds to execute" + "\n");
			fw.close();
		} catch (IOException ioe) {
			System.err.println("IOException: " + ioe.getMessage());
		}

	}

	// --------------------------------------------------------------------------------------------
	//  Single Source Shortest Path UDFs
	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("serial")
	private static final class InitVertices implements MapFunction<Long, Double>{

		private long srcId;

		public InitVertices(long srcId) {
			this.srcId = srcId;
		}

		public Double map(Long id) {
			if (id.equals(srcId)) {
				return 0.0;
			}
			else {
				return Double.POSITIVE_INFINITY;
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class CalculateDistances extends GatherFunction<Double, NullValue, Double> {

		public Double gather(Neighbor<Double,  NullValue> neighbor) {
			return neighbor.getNeighborValue() + 1;
		}

	}

	@SuppressWarnings("serial")
	private static final class ChooseMinDistance extends SumFunction<Double, NullValue, Double> {

		public Double sum(Double newValue, Double currentValue) {
			return Math.min(newValue, currentValue);
		}
	}

	@SuppressWarnings("serial")
	private static final class UpdateDistance extends ApplyFunction<Long, Double, Double> {

		public void apply(Double newDistance, Double oldDistance) {
			if (newDistance < oldDistance) {
				setResult(newDistance);
			}
		}

	}

	// --------------------------------------------------------------------------------------------
	//  Util methods
	// --------------------------------------------------------------------------------------------
	private static class HashPartitioner<T> implements Partitioner<T> {
		private static final long serialVersionUID = 1L;
		CustomKeySelector2 keySelector;
		private static final int MAX_SHRINK = 100;
		private double seed;
		private int shrink;
		public HashPartitioner(CustomKeySelector2 keySelector)
		{
			this.keySelector = keySelector;
			System.out.println("createdsfsdfsdfsdf");
			this.seed = Math.random();
			Random r = new Random();
			shrink = r.nextInt(MAX_SHRINK);

		}

		@Override
		public int partition(Object key, int numPartitions) {
			//return MathUtils.murmurHash(key.hashCode()) % numPartitions;
			return Math.abs((int) (  (int) Integer.parseInt(key.toString())*seed*shrink) % numPartitions);


		}

	}

	private static boolean fileOutput = false;
	private static Long srcVertexId = 1l;

	private static String edgesInputPath = null;

	private static String outputPath = null;

	private static String logPath = null;

	private static int maxIterations = 5;
	private static int k = 4;

	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			if(args.length != 6) {
				System.err.println("Usage: GSASSSPHash <source vertex id>" +
										   " <input edges path> <output path> <log>  <num iterations> <no. of partitions>");
				return false;
			}

			fileOutput = true;
			srcVertexId = Long.parseLong(args[0]);
			edgesInputPath = args[1];
			outputPath = args[2];
			logPath = args[3];
			maxIterations = Integer.parseInt(args[4]);
			k = Integer.parseInt(args[5]);
		} else {
			System.out.println("Executing GSASingle Source Shortest Paths example "
									   + "with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("Usage: GSASSSPHash <source vertex id>" +
									   " <input edges path> <output path> <log path><num iterations> <no. of partitions>");
		}
		return true;
	}






	@Override
	public String getDescription() {
		return "GSA Single Source Shortest Paths";
	}

}

