package org.apache.flink.graph.streaming.partitioner.edgepartitioners.batchapp;

/**
 * Created by zainababbas on 17/04/2017.
 */

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.graph.streaming.partitioner.edgepartitioners.keyselector.CustomKeySelector;
import org.apache.flink.types.NullValue;

import java.util.Random;


public class SingleSourceShortestPaths implements ProgramDescription {

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();



		DataSet<Edge<Long, NullValue>> data = env.readTextFile("/Users/zainababbas/working/gelly-streaming/BIGGGGGG/app/appmovf").map(new MapFunction<String, Edge<Long, NullValue>>() {

			@Override
			public Edge<Long, NullValue> map(String s) {
				String[] fields = s.split("\\,");
				long src = Long.parseLong(fields[0]);
				long trg = Long.parseLong(fields[1]);
				return new Edge<>(src, trg, NullValue.getInstance());
			}
		});


	//	DataSet<Edge<Long, NullValue>> partitionedData =
	//			data.partitionCustom(new HashPartitioner<>(new CustomKeySelector(0)), new CustomKeySelector<>(0));


		Graph<Long, Double, NullValue> graph = Graph.fromDataSet(data.partitionCustom(new HashPartitioner<>(new CustomKeySelector(0)), new CustomKeySelector<>(0)), new InitVertices(srcVertexId), env);

		// Execute the scatter-gather iteration
		Graph<Long, Double, NullValue> result = graph.runScatterGatherIteration(
				new MinDistanceMessenger(), new VertexDistanceUpdater(), maxIterations);

		// Extract the vertices as the result
		DataSet<Vertex<Long, Double>> singleSourceShortestPaths = result.getVertices();

		// emit result
		if (fileOutput) {
			singleSourceShortestPaths.writeAsCsv(outputPath, "\n", ",");

			// since file sinks are lazy, we trigger the execution explicitly
			env.execute("Single Source Shortest Paths Example");
		} else {
			singleSourceShortestPaths.print();
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

	/**
	 * Distributes the minimum distance associated with a given vertex among all
	 * the target vertices summed up with the edge's value.
	 */
	@SuppressWarnings("serial")
	private static final class MinDistanceMessenger extends ScatterFunction<Long, Double, Double, NullValue> {

		@Override
		public void sendMessages(Vertex<Long, Double> vertex) {
			if (vertex.getValue() < Double.POSITIVE_INFINITY) {
				for (Edge<Long, NullValue> edge : getEdges()) {
					sendMessageTo(edge.getTarget(), vertex.getValue()+1);
				}
			}
		}
	}

	/**
	 * Function that updates the value of a vertex by picking the minimum
	 * distance from all incoming messages.
	 */
	@SuppressWarnings("serial")
	private static final class VertexDistanceUpdater extends GatherFunction<Long, Double, Double> {

		@Override
		public void updateVertex(Vertex<Long, Double> vertex, MessageIterator<Double> inMessages) {

			Double minDistance = Double.MAX_VALUE;

			for (double msg : inMessages) {
				if (msg < minDistance) {
					minDistance = msg;
				}
			}

			if (vertex.getValue() > minDistance) {
				setNewVertexValue(minDistance);
			}
		}
	}
	private static class HashPartitioner<T> implements Partitioner<T> {
		private static final long serialVersionUID = 1L;
		CustomKeySelector keySelector;
		private static final int MAX_SHRINK = 100;
		private double seed;
		private int shrink;
		public HashPartitioner(CustomKeySelector keySelector)
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
	// ******************************************************************************************************************
	// UTIL METHODS
	// ******************************************************************************************************************

	private static boolean fileOutput = false;

	private static Long srcVertexId = 1l;

	private static String edgesInputPath = null;

	private static String outputPath = null;

	private static int maxIterations = 5;

	private static boolean parseParameters(String[] args) {

		if(args.length > 0) {
			if(args.length != 4) {
				System.err.println("Usage: SingleSourceShortestPaths <source vertex id>" +
										   " <input edges path> <output path> <num iterations>");
				return false;
			}

			fileOutput = true;
			srcVertexId = Long.parseLong(args[0]);
			edgesInputPath = args[1];
			outputPath = args[2];
			maxIterations = Integer.parseInt(args[3]);
		} else {
			System.out.println("Executing Single Source Shortest Paths example "
									   + "with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("Usage: SingleSourceShortestPaths <source vertex id>" +
									   " <input edges path> <output path> <num iterations>");
		}
		return true;
	}



	@Override
	public String getDescription() {
		return "Scatter-gather Single Source Shortest Paths";
	}
}
