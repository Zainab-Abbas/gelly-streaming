package org.apache.flink.graph.streaming.partitioner.tests;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.EdgesFold;
import org.apache.flink.graph.streaming.GraphStream;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.graph.streaming.WindowGraphAggregation;
import org.apache.flink.graph.streaming.summaries.DisjointSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by zainababbas on 23/06/16.
 */
public class LeastAConnectedp {


		public static void main(String[] args) throws Exception {

			if (!parseParameters(args)) {
				return;
			}

			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			DataStream<Edge<Long, NullValue>> edges = getGraphStream(env);
			GraphStream<Long, NullValue, NullValue> graph = new SimpleEdgeStream<>(edges.partitionCustom(new LeastCostAdvancePartitioner(new CustomKeySelector(0), vertexCount, edgeCount), new CustomKeySelector(0)),env);

			DataStream<DisjointSet<Long>> cc2 = graph.aggregate(new ConnectedComponentss<Long, NullValue>(5000000));

			cc2.writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE);

			JobExecutionResult result = env.execute("My Flink Job");

			try {
				FileWriter fw = new FileWriter(log, true); //the true will append the new data
				fw.write("The job took " + result.getNetRuntime(TimeUnit.SECONDS) + " seconds to execute"+"\n");//appends the string to the file
				fw.write("The job took " + result.getNetRuntime(TimeUnit.NANOSECONDS) + " nanoseconds to execute"+"\n");
				fw.close();
			} catch (IOException ioe) {
				System.err.println("IOException: " + ioe.getMessage());
			}

			System.out.println("abc");
		}

		private static String InputPath = null;
		private static String outputPath = null;
		private static String log = null;
		private static int vertexCount = 100;
		private static int edgeCount = 1000;
		private static String file1 = null;
		private static String file2 = null;
		private static int count = 0;

		private static boolean parseParameters(String[] args) {

			if (args.length > 0) {
				if (args.length != 7) {
					System.err.println("Usage: DegreeBasedAdvacned <input edges path> <output path> <log> <vertex count> <edge count>");
					return false;
				}

				InputPath = args[0];
				outputPath = args[1];
				log = args[2];
				vertexCount = Integer.parseInt(args[3]);
				edgeCount = Integer.parseInt(args[4]);
				file1 = args[5];
				file2 = args[6];

			} else {
				System.out.println("Executing example with default parameters and built-in default data.");
				System.out.println("  Provide parameters to read input data from files.");
				System.out.println(" Usage: DegreeBasedAdvacned <input edges path> <output path> <log> <vertex count> <edge count>");
			}
			return true;
		}

	private static class CustomKeySelector<K, EV> implements KeySelector<Edge<K, EV>, K> {
		private final int key1;
		private EV key2;
		private static final HashMap<Object, Object> KeyMap = new HashMap<>();

		public CustomKeySelector(int k) {
			this.key1 = k;
		}

		public K getKey(Edge<K, EV> edge) throws Exception {
			KeyMap.put(edge.getField(key1), edge.getField(key1 + 1));
			return edge.getField(key1);
		}

		public EV getValue(Object k) throws Exception {
			key2 = (EV) KeyMap.get(k);
			KeyMap.clear();
			return key2;

		}
	}


	///////code for partitioner/////////
	private static class LeastCostAdvancePartitioner<K, EV, T> implements Partitioner<T> {
		private static final long serialVersionUID = 1L;
		CustomKeySelector<T, ?> keySelector;
		int n;  // no. of nodes
		int m; // no. of edges
		private final HashMap<Long, List<Long>> vertices = new HashMap<>();  //for <partition.no, vertexId>
		private final List<Long> load = new ArrayList<>(); //for load of each partiton
		private final List<Double> cost = new ArrayList<>();
		private Long k;   //no. of partitions

		private double alpha = 0;  //parameters for formula
		private double gamma = 0;

		public LeastCostAdvancePartitioner(CustomKeySelector keySelector, int n, int m) {
			this.keySelector = keySelector;
			this.k = (long) 4;
			this.n = n;
			this.m = m;
			this.alpha = (m * Math.pow(n, 1.5)) + ((Math.pow(k, 0.5))) / Math.pow(n, 1.5);
			this.gamma = 1.5;

		}

		@Override
		public int partition(Object key, int numPartitions) {

			Long target = 0L;
			try {
				target = (Long) keySelector.getValue(key);
			} catch (Exception e) {
				e.printStackTrace();
			}

			long source = (long) key;

			int h = 0;

			if (vertices.isEmpty()) {
				for (int j = 0; j < k; j++) {
					load.add(j, (long) 0);
				}
				load.set(0, (long) 1);
				List<Long> L = new ArrayList<>();
				L.add(source);
				L.add(target);
				vertices.put((long) 0, L);
				h = 0;

			} else {
				//condition 1 both vertices in same partition
				for (int j = 0; j < k; j++) {
					cost.add(j, (double) 0);
				}
				for (int i = 0; i < k; i++) {

					int c = 0;
					c = getValue(source, target, i);
					cost.set(i, (double) ((double) c - alpha * gamma * Math.pow(load.get(i), gamma - 1)));

					//get the value of S(k) from each partition using return
				}

				h = CompareCost();

				Long l = load.get(h);
				l++;
				load.set(h, l);
				if (vertices.get((long) h) != null) {
					List<Long> L = vertices.get((long) h);
					if (!L.contains(source)) {
						L.add(source);
					}
					if (!L.contains(target)) {
						L.add(target);
					}
					vertices.put((long) h, L);
				} else {
					List<Long> L = new ArrayList<>();
					L.add(source);
					L.add(target);
					vertices.put((long) h, L);
				}
				cost.clear();
			}

			return h;
		}

		public int CompareCost() {
			Double max = cost.get(0);
			int sub = 0;
			for (int j = 1; j < k; j++) {

				if (max .compareTo( cost.get(j)) < 0) {
					max = cost.get(j);
					sub = j;
				}
			}
			return sub;
		}

		public int getValue(Long source, Long target, int p) {
			int i = 0;
			List<Long> L;
			L = vertices.get((long) p);
			if (L != null) {
				if (L.contains(source) && L.contains(target)) {

					i = 2;
				} else if (L.contains(source) && !L.contains(target)) {

					i = 1;
				} else if (!L.contains(source) && L.contains(target)) {

					i = 1;
				} else {
					i = 0;
				}

			}
			return i;
		}


	}


		private static DataStream<Edge<Long, NullValue>> getGraphStream(StreamExecutionEnvironment env) throws IOException {
			return env.fromCollection(getEdges());
		}

		public static final List<Edge<Long, NullValue>> getEdges() throws IOException {
			List<Edge<Long, NullValue>> edges = new ArrayList<>();

			FileReader inputFile = new FileReader(InputPath);
			//Instantiate the BufferedReader Class

			BufferedReader bufferReader = new BufferedReader(inputFile);

			String line;
			// Read file line by line and print on the console
			while ((line = bufferReader.readLine()) != null) {
				String[] fields = line.split("\\,");
				long src = Long.parseLong(fields[0]);
				long trg = Long.parseLong(fields[1]);

				edges.add(new Edge<>(src, trg, NullValue.getInstance()));
			}


			return edges;
		}

		private static class ConnectedComponentss<K extends Serializable, EV> extends WindowGraphAggregation<K, EV, DisjointSet<K>, DisjointSet<K>> implements Serializable {

			private long mergeWindowTime;

			public ConnectedComponentss(long mergeWindowTime) {

				super(new UpdateCC(), new CombineCC(), new DisjointSet<K>(), mergeWindowTime, false);
			}


			public final static class UpdateCC<K extends Serializable> implements EdgesFold<K, NullValue, DisjointSet<K>> {

				@Override
				public DisjointSet<K> foldEdges(DisjointSet<K> ds, K vertex, K vertex2, NullValue edgeValue) throws Exception {
					ds.union(vertex, vertex2);
					return ds;
				}
			}

			public static class CombineCC<K extends Serializable> implements ReduceFunction<DisjointSet<K>> {

				@Override
				public DisjointSet<K> reduce(DisjointSet<K> s1, DisjointSet<K> s2) throws Exception {

					count++;
					int count1 = s1.getMatches().size();
					int count2 = s2.getMatches().size();

					try {

						FileWriter fw1 = new FileWriter(file1, true); //the true will append the new data
						fw1.write(count1 + "\n");//appends the string to the file
						fw1.close();

					} catch (IOException ioe) {
						System.err.println("IOException: " + ioe.getMessage());
					}

					if (count1 > count2) {
						try {

							FileWriter fw2 = new FileWriter(file2, true); //the true will append the new data
							fw2.write(count1 + "\n");//appends the string to the file
							fw2.close();

						} catch (IOException ioe) {
							System.err.println("IOException: " + ioe.getMessage());
						}
					}
					if (count1 <= count2) {
						try {
							FileWriter fw2 = new FileWriter(file2, true); //the true will append the new data
							fw2.write(count2 + "\n");//appends the string to the file
							fw2.close();
						}
						catch (IOException ioe) {
							System.err.println("IOException: " + ioe.getMessage());}
						s2.merge(s1);
						return s2;
					}
					s1.merge(s2);
					return s1;
				}
			}
		}

	}
