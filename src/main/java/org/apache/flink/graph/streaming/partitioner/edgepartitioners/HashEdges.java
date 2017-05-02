package org.apache.flink.graph.streaming.partitioner.edgepartitioners;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.partitioner.edgepartitioners.keyselector.CustomKeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.MathUtils;

import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by zainababbas on 07/02/2017.
 */
public class HashEdges {

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStream<Edge<Long, NullValue>> edges = getGraphStream(env);

		edges.partitionCustom(new HashPartitioner<>(new CustomKeySelector(0)), new CustomKeySelector<>(0)).writeAsCsv(outputPath, FileSystem.WriteMode.OVERWRITE).setParallelism(k);

	//	edges.partitionCustom(new HashPartitioner<>(new CustomKeySelector(0)), new CustomKeySelector<>(0))
	//			.addSink(new TimestampingSink(outputPath)).setParallelism(k);
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
	private static int k = 0;
	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			if (args.length != 4) {
				System.err.println("Usage: HashEdges <input edges path> <output path> <log> <partitions> ");
				return false;
			}

			InputPath = args[0];
			outputPath = args[1];
			log = args[2];
			k = (int) Long.parseLong(args[3]);
		} else {
			System.out.println("Executing example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println(" Usage: HashEdges <input edges path> <output path> <log> <partitions>");
		}
		return true;
	}



	///////code for partitioner/////////
	private static class HashPartitioner<T> implements Partitioner<T> {
		private static final long serialVersionUID = 1L;
		CustomKeySelector keySelector;
	/*	private double seed;
		private int shrink;
		private static final int MAX_SHRINK = 100;
		private int k;*/
		public HashPartitioner(CustomKeySelector keySelector)
		{
			this.keySelector = keySelector;
		//	this.seed = Math.random();
		//	Random r = new Random();
		//	shrink = r.nextInt(MAX_SHRINK);
		//	this.k=k;
		}

		@Override
		public int partition(Object key, int numPartitions) {
			//long source = (long) key;
			//return Math.abs((int) ( (int) (source)*seed*shrink) % k);
				return MathUtils.murmurHash(key.hashCode()) % numPartitions;


		}

	}

	public static  DataStream<Edge<Long, NullValue>> getGraphStream(StreamExecutionEnvironment env) throws IOException {

		return env.readTextFile(InputPath)
					   .map(new MapFunction<String, Edge<Long, NullValue>>() {
						   @Override
						   public Edge<Long, NullValue> map(String s) throws Exception {
							   String[] fields = s.split("\\,");
							   long src = Long.parseLong(fields[0]);
							   long trg = Long.parseLong(fields[1]);
							   return new Edge<>(src, trg, NullValue.getInstance());
						   }
					   });

	}

}

