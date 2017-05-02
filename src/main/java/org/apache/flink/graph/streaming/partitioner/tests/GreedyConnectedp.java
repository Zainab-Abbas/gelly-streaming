package org.apache.flink.graph.streaming.partitioner.tests;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
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
import org.apache.flink.util.Collector;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by zainababbas on 22/06/16.
 */
public class GreedyConnectedp {


		public static void main(String[] args) throws Exception {

			if (!parseParameters(args)) {
				return;
			}

			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			DataStream<Tuple2<Long, List<Long>>> vertices = getGraphStream(env);

			//////////////////////

			GraphStream<Long, NullValue, NullValue> graph = new SimpleEdgeStream<>(vertices
																						   .partitionCustom(new Greedy<>(new CustomKeySelector(0), vertexCount), new CustomKeySelector(0))
																						   .flatMap((new FlatMapFunction<Tuple2<Long, List<Long>>, Edge<Long, NullValue>>() {
																							   @Override
																							   public void flatMap(Tuple2<Long, List<Long>> value, Collector<Edge<Long, NullValue>> out) throws Exception {
																								   value.f1.forEach(s -> {
																									   Edge<Long, NullValue> edge = new Edge<>(value.f0, s, NullValue.getInstance());
																									   out.collect(edge);
																								   });
																								   out.close();
																							   }
																						   })), env);
			////////////////////////////

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
		private static String file1 = null;
		private static String file2 = null;
		private static int count = 0;

		private static boolean parseParameters(String[] args) {

			if (args.length > 0) {
				if (args.length != 6) {
					System.err.println("Usage: LinearGreedyLog <input edges path> <output path> <vertex count>");
					return false;
				}

				InputPath = args[0];
				outputPath = args[1];
				log = args[2];
				vertexCount = Integer.parseInt(args[3]);
				file1 = args[4];
				file2 = args[5];

			} else {
				System.out.println("Executing example with default parameters and built-in default data.");
				System.out.println("  Provide parameters to read input data from files.");
				System.out.println("  See the documentation for the correct format of input files.");
				System.out.println("  Usage: LinearGreedyLog <input edges path> <output path> <vertex count>");
			}
			return true;
		}

		private static DataStream<Tuple2<Long, List<Long>>> getGraphStream(StreamExecutionEnvironment env) throws IOException {

			return env.fromCollection(getVertices());
		}

		public static final List<Tuple2<Long, List<Long>>> getVertices() throws IOException {
			List<Tuple2<Long, List<Long>>> vertices = new ArrayList<>();
			FileReader inputFile = new FileReader(InputPath);

			BufferedReader bufferReader = new BufferedReader(inputFile);

			String line;

			while ((line = bufferReader.readLine()) != null) {
				String[] fields = line.split("\\[");
				String src = fields[0];
				int h=src.indexOf(',');
				src=src.substring(1,h);
				Long source = Long.parseLong(src);
				String trg= fields[1];

				int n = trg.indexOf("]");
				String j = trg.substring(0,n);
				String fg =j.replaceAll("\\s","");
				String[] ne = fg.split("\\,");
				int f = ne.length;
				List<Long> neg = new ArrayList<Long>();
				neg.add(Long.parseLong(ne[0]));
				for (int k = 1; k < f; k++) {
					neg.add(Long.parseLong(String.valueOf(ne[k])));


				}
				vertices.add(new Tuple2<>(source, neg));
			}

			//Close the buffer reader
			bufferReader.close();

			return vertices;
		}

		/////key selector /////////////////
		private static class CustomKeySelector<K, EV> implements KeySelector<Tuple2<K, List<EV>>, K> {
			private final K key1;
			private EV key2;
			private static final HashMap<Object, Object> DoubleKey = new HashMap<>();

			public CustomKeySelector(K k) {
				this.key1 = k;
			}

			public K getKey(Tuple2<K, List<EV>> vertices) throws Exception {
				DoubleKey.put((vertices.getField((Integer) key1)), vertices.getField((Integer) key1 + 1));
				return vertices.getField((Integer) key1);
			}

			public EV getValue(Object k) throws Exception {
				key2 = (EV) DoubleKey.get(k);
				DoubleKey.clear();
				return key2;
			}
		}

		///////code for partitioner/////////
		private static class Greedy<K, EV, T> implements Partitioner<T> {
			private static final long serialVersionUID = 1L;
			private final HashMap<Long, List<Long>> Result = new HashMap<>();//partitionid, list of vertices placed
			private final List<Long> load = new ArrayList<>(); //for load of each partiton
			private final List<Tuple2<Long, Long>> edges = new ArrayList<>();
			CustomKeySelector<T, ?> keySelector;
			private Long k;  //no. of partitions
			private Double C;     // no. of vertices/total no. of partitions

			public Greedy(CustomKeySelector<T, ?> keySelector, int m) {
				this.keySelector = keySelector;
				this.k = (long) 4;
				this.C = (double) m / (double) k;

			}

			@Override
			public int partition(Object key, int numPartitions) {

				List<Long> neighbours = new ArrayList<>();
				try {
					neighbours = (List<Long>) keySelector.getValue(key);
				} catch (Exception e) {
					e.printStackTrace();
				}

				long source = (long) key;

				int h = 0;

				if (Result.isEmpty()) {
					for (int j = 0; j < k; j++) {
						load.add(j, (long) 0);
					}
					load.set(0, (long) 1);
					load.set(0, (long) 1);
					List<Long> L = new ArrayList<>();
					L.add(source);
					Result.put((long) 0, L);
					h = 0;

				} else {
					List<Double> num = new ArrayList<>();
					int n = 0;
					for (int j = 0; j < k; j++) {
						num.add(j, 0.0);
					}
					for (int i = 0; i < k; i++) {
						n = getValue(i, neighbours);
						num.set(i, (double) (n * (1 - (load.get(i) / C))));   //1.25 is C = total vertices/no.of partitions
					}

					Long l = 0L;
					int index = 0;
					Double I = num.get(0);


					for (int i = 1; i < k; i++) {
						if (I.compareTo(num.get(i)) < 0) {
							I = num.get(i);
							index = i;
						}
						Double J = num.get(i);

						if (I.compareTo(J) == 0) {
							if (load.get(i) < load.get(index)) {
								I = num.get(i);
								index = i;
							}
						}

					}
					h = index;
					l = load.get(index);
					l = l + 1;
					load.set(index, l);
					if (Result.get((long) index) == null) {
						List<Long> L = new ArrayList<>();
						L.add(source);
						Result.put((long) index, L);
					} else {
						List<Long> L;
						L = Result.get((long) index);
						L.add(source);
						Result.put((long) index, L);
					}
				}
				return h;
			}

			public int getValue(int p, List<Long> n) {

				int ne = 0;
				List<Long> list;
				for (int i = 0; i < n.size(); i++) {
					Long v = n.get(i);
					list = Result.get((long) p);
					if (list != null) {
						if (list.contains(v)) {
							ne++;
						}
					}
				}
				return ne;
			}

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
