package org.apache.flink.graph.streaming.partitioner.tests;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by zainababbas on 21/06/16.
 */
public class FennelConnectedn {


	public static void main(String[] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple2<Long, List<Long>>> vertices = getGraphStream(env);

		//////////////////////

			GraphStream<Long, NullValue, NullValue> graph = new SimpleEdgeStream<>(vertices.flatMap((new FlatMapFunction<Tuple2<Long, List<Long>>, Edge<Long, NullValue>>() {
				@Override
				public void flatMap(Tuple2<Long, List<Long>> value, Collector<Edge<Long, NullValue>> out) throws Exception {
					value.f1.forEach(s -> {
						Edge<Long, NullValue> edge = new Edge<>(value.f0, s, NullValue.getInstance());
						out.collect(edge);
					});
					out.close();
				}
			})),env);

		////////////////////////////

		DataStream<DisjointSet<Long>> cc2 = graph.aggregate(new ConnectedComponentss<Long, NullValue>(5000000));

        cc2.writeAsText(output, FileSystem.WriteMode.OVERWRITE);
cc2.flatMap(new FlatMapFunction<DisjointSet<Long>, Long>() {

	@Override
	public void flatMap(DisjointSet<Long> longDisjointSet, Collector<Long> collector) throws Exception {
		collector.collect((long) longDisjointSet.getMatches().size());
		collector.close();

	}
}).print();
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



	private static DataStream<Tuple2<Long, List<Long>>> getGraphStream(StreamExecutionEnvironment env) throws IOException {

		return env.fromCollection(getVertices());
	}

	private static String InputPath = null;
	private static String output = null;
	private static String log = null;
	private static String file1 = null;
	private static String file2 = null;
	private static int count = 0;

	private static boolean parseParameters(String[] args) {

		if(args.length > 0) {
			if(args.length != 5) {
				System.err.println("Usage: FennelLog <input edges path> <log path> <size file> <value file>");
				return false;
			}

			InputPath = args[0];
			output = args[1];
			log = args[2];
			file1 = args[3];
			file2 = args[4];
		} else {
			System.out.println("Executing example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  Usage: FennelLog <input edges path> <log path> <size file> <value file>");
		}
		return true;
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

			long n = trg.indexOf("]");
			String j = trg.substring(0, (int) n);
			String fg =j.replaceAll("\\s","");
			String[] ne = fg.split("\\,");
			int f = ne.length;
			List<Long> neg = new ArrayList<Long>();
			neg.add(Long.parseLong(ne[0]));
			for (int k = 1; k < f; k++) {
				neg.add(Long.parseLong(String.valueOf(ne[k])));

			}
			vertices.add(new Tuple2<>(source,neg));
		}



		//Close the buffer reader
		bufferReader.close();

		return vertices;
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
