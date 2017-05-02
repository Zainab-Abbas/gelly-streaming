package org.apache.flink.graph.streaming.partitioner.vertexpartitioners;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.streaming.partitioner.vertexpartitioners.keyselector.CustomKeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.MathUtils;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by zainababbas on 27/04/16.
 */

public class HashVertices {

	public static void main(String[] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStream<Tuple2<Long, List<Long>>> vertices = getVertices(env);


		vertices.partitionCustom(new HashPartitioner<>(new CustomKeySelector<>(0)), new CustomKeySelector<>(0)).writeAsCsv(outputPath, FileSystem.WriteMode.OVERWRITE).setParallelism(k);
		//vertices.partitionCustom(new HashPartitioner<>(new CustomKeySelector<>(0)), (new CustomKeySelector<>(0))).addSink(new TimestampingSinkv(outputPath)).setParallelism(k);

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

		if(args.length > 0) {
			System.out.println(args.length);
			if(args.length != 4) {
				System.err.println("Usage: HashVerticesLog <input edges path> <output path> <log> <partitions>");
				return false;
			}

			InputPath = args[0];
			outputPath = args[1];
			log = args[2];
			k = (int) Long.parseLong(args[3]);
		} else {
			System.out.println("Executing example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  Usage: HashVerticesLog <input edges path> <output path> <log> <partitions>");
		}
		return true;
	}

	public static DataStream<Tuple2<Long, List<Long>>> getVertices(StreamExecutionEnvironment env) throws IOException {

		List<Tuple2<Long, List<Long>>> vertices = new ArrayList<>();

		return env.readTextFile(InputPath)
					   .map(new MapFunction<String, Tuple2<Long, List<Long>>>() {
						   @Override
						   public Tuple2<Long, List<Long>> map(String s) throws Exception {
							   String[] fields = s.split("\\[");
							   String src = fields[0];
							   int h=src.indexOf(':');
							   src=src.substring(0,h);
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
							   return new Tuple2<Long, List<Long>>(source, neg);
						   }
					   });

	}


	///////code for partitioner/////////

	private static class HashPartitioner<T> implements Serializable, Partitioner<T> {
		private static final long serialVersionUID = 1L;
		CustomKeySelector keySelector;

		public HashPartitioner(CustomKeySelector keySelector) {
			this.keySelector = keySelector;
		}

		@Override
		public int partition(Object key, int numPartitions) {

			return MathUtils.murmurHash(key.hashCode()) % numPartitions;

		}


	}

}

