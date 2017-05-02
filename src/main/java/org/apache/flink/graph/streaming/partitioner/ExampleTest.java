package org.apache.flink.graph.streaming.partitioner;

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
import java.util.List;


public class ExampleTest {

	public static void main(String[] args) throws Exception {
		if(!parseParameters(args)) {
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Edge<Long, NullValue>> edges = getGraphStream(env);
		long startTime = System.nanoTime();

		//edges.partitionCustom(new test(new SampleKeySelector(0)), new SampleKeySelector(0)).writeAsCsv("/Users/zainababbas/gelly3/gelly-streaming/output1", FileSystem.WriteMode.OVERWRITE);
		edges.partitionCustom(new test(new SampleKeySelector(0)), new SampleKeySelector(0)).writeAsCsv(outputPath, FileSystem.WriteMode.OVERWRITE);
		//edges.partitionCustom(new test(new SampleKeySelector(0)), new SampleKeySelector(0)).print();
		long endTime = System.nanoTime();
		long duration = (endTime - startTime);
		try
		{
			FileWriter fw = new FileWriter(log,true); //the true will append the new data
			fw.write(duration*Math.pow( 10, -9 ) +"\n");//appends the string to the file
			fw.write(duration+"\n");
			fw.close();
		}
		catch(IOException ioe)
		{
			System.err.println("IOException: " + ioe.getMessage());
		}

		System.out.println(duration);
		System.out.println("sfsdfsfsfs");
		//Graph<LongValue,NullValue,NullValue> graph = new Graph<>()
	/*	SingleOutputStreamOperator g = edges.flatMap(new FlatMapFunction<Edge<Long,NullValue>, Long>() {
			@Override
			public void flatMap(Edge<Long, NullValue> value, Collector<Long> out) throws Exception {
				 out.collect(value.getTarget());
				out.close();
			}

		});
g.print();*/
		//edges.partitionCustom(new test(new SampleKeySelector(0)),new SampleKeySelector(0)).print();
//		GraphStream<Long, NullValue, NullValue> graph = new SimpleEdgeStream<>(edges.partitionCustom(new test(new SampleKeySelector(0)),new SampleKeySelector(0)), env);
//		DataStream<DisjointSet<Long>> cc = graph.aggregate(new ConnectedComponents<Long, NullValue>(5000));
 //       cc.print();
//
//		DataStream<Edge<Long, NullValue>> edges2 = getGraphStream(env);
		//edges.partitionCustom(new test(new SampleKeySelector(0)),new SampleKeySelector(0)).print();
//		GraphStream<Long, NullValue, NullValue> graph2 = new SimpleEdgeStream<>(edges2, env);
//		DataStream<DisjointSet<Long>> cc2 = graph.aggregate(new ConnectedComponents<Long, NullValue>(5000));
//		cc2.print();
		/*cc.flatMap(new WindowTriangles.FlattenSet()).keyBy(0)
				.timeWindow(Time.of(2000, TimeUnit.MILLISECONDS))
				.fold(new Tuple2<Long, Long>(0l, 0l), new WindowTriangles.IdentityFold()).print();*/

		env.execute("testing custom partitioner");
		}

    private static class SampleKeySelector<K, EV> implements KeySelector<Edge<K, EV>, K> {
	private final int key;

	public SampleKeySelector(int k) {
		this.key = k;
	}

	public K getKey(Edge<K, EV> edge) throws Exception {
		//System.out.println((Long) edge.getField(key));
		return edge.getField(key);
	}
}
	private static class test<T> implements Partitioner<T> {
		private static final long serialVersionUID = 1L;

		//private int[] returnArray = new int[1];
		KeySelector<T, ?> keySelector;

		public test(KeySelector<T, ?> keySelector) {
			this.keySelector = keySelector;
		}

	/*	public int[] selectChannels(SerializationDelegate<StreamRecord<T>> record,
									int numberOfOutputChannels) {
			object key;
			try {
				key =2;
				//key = keySelector.getKey(record.getInstance().getValue());
			} catch (Exception e) {
				throw new RuntimeException("Could not extract key from " + record.getInstance().getValue(), e);
			}
			returnArray[0] = (int) key;

			return returnArray;
		}


		@Override
		public String toString() {
			return "new";
		}*/

		@Override
		public int partition(Object key, int numPartitions) {
			//System.out.println(numPartitions);
			long k = (long) key;
			int h = (int) k;

			return h%4;
		}
	}


	private static String edgeInputPath = null;
	private static String outputPath = null;
	private static String log = null;
	private static int vertexCount = 100;
	private static int edgeCount = 1000;
	private static boolean parseParameters(String[] args) {

		if(args.length > 0) {
			if(args.length != 5) {
				System.err.println("Usage: ExampleTest <input edges path> <output path> <log> <vertex count> <edge count>");
				return false;
			}

			edgeInputPath = args[0];
			outputPath = args[1];
			log = args[2];
			vertexCount = Integer.parseInt(args[3]);
			edgeCount = Integer.parseInt(args[4]);
		} else {
			System.out.println("Executing example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage: BroadcastTriangleCount <input edges path> <output path> <vertex count> <edge count>");
		}
		return true;
	}

	private static DataStream<Edge<Long, NullValue>> getGraphStream(StreamExecutionEnvironment env) throws IOException {

	/*	return env.readTextFile(edgeInputPath)
					   .map(new MapFunction<String, Edge<Long, NullValue>>() {
						   @Override
						   public Edge<Long, NullValue> map(String s) throws Exception {

							   String[] fields = s.split("\\,");
							   long src = Long.parseLong(fields[0]);
							   long trg = Long.parseLong(fields[1]);
							   if(src!=trg) {


								   return new Edge<>(src, trg, NullValue.getInstance());
							   }
						   }
					   });
*/
		return env.fromCollection(getEdges());
	}

	public static final List<Edge<Long, NullValue>> getEdges() throws IOException {
		List<Edge<Long, NullValue>> edges = new ArrayList<>();

		FileReader inputFile = new FileReader(edgeInputPath);
		//Instantiate the BufferedReader Class

		BufferedReader bufferReader = new BufferedReader(inputFile);

		String line;
		// Read file line by line and print on the console
		while ((line = bufferReader.readLine()) != null) {
			String[] fields = line.split("\\,");
			long src = Long.parseLong(fields[0]);
			long trg = Long.parseLong(fields[1]);

				edges.add(new Edge<>(src, trg, NullValue.getInstance()));}


		return edges;
	}

}

