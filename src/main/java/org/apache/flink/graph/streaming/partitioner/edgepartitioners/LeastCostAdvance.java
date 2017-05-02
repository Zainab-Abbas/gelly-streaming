package org.apache.flink.graph.streaming.partitioner.edgepartitioners;

/**
 * Created by zainababbas on 27/04/16.
 */

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class LeastCostAdvance {

	public static void main(String[] args) throws Exception {
		if (!parseParameters(args)) {
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Edge<Long, NullValue>> edges = getGraphStream(env);


		edges.partitionCustom(new LeastCostAdvancePartitioner(new CustomKeySelector(0), vertexCount, edgeCount), new CustomKeySelector(0)).writeAsCsv(outputPath, FileSystem.WriteMode.OVERWRITE);

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

	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			if (args.length != 5) {
				System.err.println("Usage: LeastCostAdvance <input edges path> <output path> <log> <vertex count> <edge count> ");
				return false;
			}

			InputPath = args[0];
			outputPath = args[1];
			log = args[2];
			vertexCount = Integer.parseInt(args[3]);
			edgeCount = Integer.parseInt(args[4]);
		} else {
			System.out.println("Executing example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println(" Usage: LeastCostAdvance <input edges path> <output path> <log> <vertex count> <edge count> ");
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
			//if(src!=trg){
			edges.add(new Edge<>(src, trg, NullValue.getInstance()));
		}
		//	}

		return edges;
	}
}


