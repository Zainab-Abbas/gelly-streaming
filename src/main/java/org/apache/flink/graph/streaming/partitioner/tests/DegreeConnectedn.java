package org.apache.flink.graph.streaming.partitioner.tests;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.ReduceFunction;
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
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by zainababbas on 22/06/16.
 */
public class DegreeConnectedn {

		public static void main(String[] args) throws Exception {

			if (!parseParameters(args)) {
				return;
			}

			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			DataStream<Edge<Long, NullValue>> edges = getGraphStream(env);
			GraphStream<Long, NullValue, NullValue> graph = new SimpleEdgeStream<>(edges,env);

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
		private static String file1 = null;
		private static String file2 = null;
		private static int count = 0;

		private static boolean parseParameters(String[] args) {

			if (args.length > 0) {
				if (args.length != 5) {
					System.err.println("Usage: DegreeBasedAdvacned <input edges path> <output path> <log> <vertex count> <edge count>");
					return false;
				}

				InputPath = args[0];
				outputPath = args[1];
				log = args[2];
				file1 = args[3];
				file2 = args[4];

			} else {
				System.out.println("Executing example with default parameters and built-in default data.");
				System.out.println("  Provide parameters to read input data from files.");
				System.out.println(" Usage: DegreeBasedAdvacned <input edges path> <output path> <log> <vertex count> <edge count>");
			}
			return true;
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
