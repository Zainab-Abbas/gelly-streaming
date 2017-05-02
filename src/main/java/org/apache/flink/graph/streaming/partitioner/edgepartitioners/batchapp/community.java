package org.apache.flink.graph.streaming.partitioner.edgepartitioners.batchapp;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.LabelPropagation;
import org.apache.flink.graph.streaming.partitioner.edgepartitioners.keyselector.CustomKeySelector;
import org.apache.flink.types.NullValue;

import java.util.Random;

/**
 * Created by zainababbas on 11/04/2017.
 */
public class community {
	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

//		StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();

//		DataStream<org.apache.flink.graph.Edge<Long, NullValue>> edges = getGraphStream(env1);
		env.setParallelism(k);

//		DataStream<org.apache.flink.graph.Edge<Long, NullValue>> partitionesedges  = edges.partitionCustom(new HashPartitioner<>(new CustomKeySelector(0)), new CustomKeySelector<>(0));


		//DataSet<Tuple3<String, String, Double>> edgeTuples = env.readCsvFile("path/to/edge/input").types(String.class, String.class, Double.class);
		//DataSource<Tuple3<String, String, Double>>  edgeTuples = env.readCsvFile("/Users/zainababbas/working/gelly-streaming/BIGGGGGG/app/appmovf").types(String.class, String.class, Double.class);

		DataSet<Edge<Long, NullValue>> data = env.readTextFile("/Users/zainababbas/working/gelly-streaming/BIGGGGGG/app/appmovf").map(new MapFunction<String, Edge<Long, NullValue>>() {

			@Override
			public Edge<Long, NullValue> map(String s) {
				String[] fields = s.split("\\,");
				long src = Long.parseLong(fields[0]);
				long trg = Long.parseLong(fields[1]);
				return new Edge<>(src, trg, NullValue.getInstance());
			}
		});


		DataSet<Edge<Long, NullValue>> partitionedData =
				data.partitionCustom(new HashPartitioner<>(new CustomKeySelector(0)), new CustomKeySelector<>(0));


		DataSet<Tuple2<Long, Long>> tuples = partitionedData.map(new MapFunction<Edge<Long, NullValue>, Tuple2<Long, Long>>() {
			@Override
			public Tuple2<Long, Long> map(Edge<Long, NullValue> edge) throws Exception {
				Long s = Long.valueOf(String.valueOf(edge.f0));
				Long t = Long.valueOf(String.valueOf(edge.f1));
				return new Tuple2<>(s, t);
			}
		});




		Graph<Long, NullValue, NullValue> graph = Graph.fromDataSet(partitionedData,env);

		DataSet<Vertex<Long,NullValue>> verticesWithCommunity = graph.run(new LabelPropagation<Long,NullValue,NullValue>(1));

// print the result


		verticesWithCommunity.print();

//		Graph<Long, Long, Long> simpleGraph = Graph.fromCsvReader("/Users/zainababbas/partitioning/gelly-streaming/DBHDOUVA", env).types(Long.class,Long.class,Long.class);


//	Graph<Long, Long, NullValue> graph =

		// run Label Propagation for 30 iterations to detect communities on the input graph
//		DataSet<Vertex<Long, Long>> verticesWithCommunity = simpleGraph.run(new LabelPropagation<>(30));

// print the result
//		verticesWithCommunity.print();

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


	private static String InputPath = null;
	private static String outputPath = null;
	private static String log = null;
	private static int k = 4;
	private static int count = 0;
	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			if (args.length != 4) {
				System.err.println("Usage: Dbh <input edges path> <output path> <log> <partitions> ");
				return false;
			}

			InputPath = args[0];
			outputPath = args[1];
			log = args[2];
			k = (int) Long.parseLong(args[3]);
		} else {
			System.out.println("Executing example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println(" Usage: Dbh <input edges path> <output path> <log> <partitions>");
		}
		return true;
	}

}
