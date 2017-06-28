package org.apache.flink.graph.streaming.partitioner.edgepartitioners.batchapp;

/**
 * Created by zainababbas on 17/04/2017.
 */

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CoGroupOperator;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.graph.streaming.partitioner.edgepartitioners.keyselector.CustomKeySelector3;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.Random;


public class SingleSourceDelta {

	public static void main(String[] args) throws Exception
	{

	/*	if (!parseParameters(args)) {
			return;
		}*/

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		final int maxIterations = 10;

		// make parameters available in the web interface
		//env.getConfig().setGlobalJobParameters(params);

		// read vertex and edge data
		DataSet<Edge<Long, NullValue>> data = env.readTextFile("/Users/zainababbas/partitioning/gelly-streaming/sample_graph.txt").map(new MapFunction<String, Edge<Long, NullValue>>() {

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


		Graph<Long, Long, NullValue> graph = Graph.fromDataSet(data, new InitVertices(0), env);

		DataSet<Long> vertices = graph.getVertices().map(new MapFunction<Vertex<Long,Long>, Long>() {
			@Override
			public Long map(Vertex<Long, Long> longLongVertex) throws Exception {
				return longLongVertex.f0;
			}
		});

		vertices.print();

		DataSet<Tuple2<Long, Long>> edges =

				graph.getEdges().flatMap(new FlatMapFunction<Edge<Long, NullValue>, Tuple2<Long, Long>>() {

					@Override
					public void flatMap(Edge<Long, NullValue> longNullValueEdge, Collector<Tuple2<Long, Long>> collector) throws Exception {
						Tuple2<Long,Long> t = new Tuple2<Long, Long>();
						t.f0=longNullValueEdge.f0;
						t.f1=longNullValueEdge.f1;
						collector.collect(t);
					}

				});

		edges.print();



		//DataSet<Long> vertices = getVertexDataSet(env, params);
		//DataSet<Tuple2<Long, Long>> edges = getEdgeDataSet(env, params).flatMap(new UndirectEdge());

		// assign the initial components (equal to the vertex id)
		DataSet<Tuple2<Long, Long>> verticesWithInitialId =
				vertices.map(new ConnectedComponents.DuplicateValue<Long>());


		// open a delta iteration
		DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration =
				verticesWithInitialId.iterateDelta(verticesWithInitialId, maxIterations, 0);

		iteration.getInitialWorkset().print();




		iteration.setSolutionSetUnManaged(true);
		CoGroupOperator messages = edges.coGroup(iteration.getWorkset())
										   .where(0).equalTo(0).with(new MinDistanceMessenger()).withPartitioner(new HashPartitioner<>(new CustomKeySelector3(0)));
		// .coGroup(iteration.getWorkset()).where(new CustomKeySelector(0)).equalTo(0).with(new VertexDistanceUpdater());





		CoGroupOperator updates =  messages
										   .coGroup(iteration.getSolutionSet()).where(0).equalTo(0).with(new VertexDistanceUpdater());
		//.where(0).equalTo(0).with(new VertexDistanceUpdater());
		DataSet result = iteration.closeWith(updates,  updates);





		//CoGroupOperator updates =  messages.coGroup(iteration.getWorkset()).where(new CustomKeySelector(0)).equalTo(0).with(new VertexDistanceUpdater());
		result.print();
		//	DataSet result = iteration.closeWith(updates,  updates);
		// Execute the scatter-gather iteration
		/*Graph<Long, Double, NullValue> result = graph.runScatterGatherIteration(
				new MinDistanceMessenger(), new VertexDistanceUpdater(), maxIterations);*/

		// Extract the vertices as the result
		//DataSet<Vertex<Long, Double>> singleSourceShortestPaths = result.getVertices();
		//result.writeAsCsv("/Users/zainababbas/working/gelly-streaming/BIGGGGGG/app/", "\n", ",");
		env.execute("Single Source Shortest Paths Example");


	}

	// --------------------------------------------------------------------------------------------
	//  Single Source Shortest Path UDFs
	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("serial")
	private static final class InitVertices implements MapFunction<Long, Long>{

		private long srcId;

		public InitVertices(long srcId) {
			this.srcId = srcId;
		}

		public Long map(Long id) {
			if (id.equals(srcId)) {
				return 0L;
			}
			else {
				return Long.MAX_VALUE;
			}
		}
	}

	/**
	 * Distributes the minimum distance associated with a given vertex among all
	 * the target vertices summed up with the edge's value.
	 */
	@SuppressWarnings("serial")
	private static final class MinDistanceMessenger extends ScatterFunction<Long, Long, Double, NullValue> implements CoGroupFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Object> {

		@Override
		public void sendMessages(Vertex<Long, Long> vertex) {
			if (vertex.getValue() < Double.POSITIVE_INFINITY) {
				for (Edge<Long, NullValue> edge : getEdges()) {
					sendMessageTo(edge.getTarget(), Double.valueOf(vertex.getValue()+1));
				}
			}
		}

		@Override
		public void coGroup(Iterable<Tuple2<Long, Long>> iterable, Iterable<Tuple2<Long, Long>> iterable1, Collector<Object> collector) throws Exception {

		}
	}

	/**
	 * Function that updates the value of a vertex by picking the minimum
	 * distance from all incoming messages.
	 */
	@SuppressWarnings("serial")
	private static final class VertexDistanceUpdater extends GatherFunction<Long, Double, Double> implements CoGroupFunction {

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


		@Override
		public void coGroup(Iterable iterable, Iterable iterable1, Collector collector) throws Exception {

		}
	}
	private static class HashPartitioner<T> implements Partitioner<T> {
		private static final long serialVersionUID = 1L;
		CustomKeySelector3 keySelector;
		private static final int MAX_SHRINK = 100;
		private double seed;
		private int shrink;
		public HashPartitioner(CustomKeySelector3 keySelector)
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


}
