package org.apache.flink.graph.streaming.partitioner.edgepartitioners.messages;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.EdgesFold;
import org.apache.flink.graph.streaming.GraphStream;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.graph.streaming.WindowGraphAggregation;
import org.apache.flink.graph.streaming.partitioner.edgepartitioners.keyselector.CustomKeySelector;
import org.apache.flink.graph.streaming.partitioner.object.StoredObject;
import org.apache.flink.graph.streaming.partitioner.object.StoredState;
import org.apache.flink.graph.streaming.summaries.DisjointSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;

import java.io.*;
import java.util.Random;


/**
 * Created by zainababbas on 07/02/2017.
 */
public class messagesD {

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStream<Edge<Long, NullValue>> edges = getGraphStream(env);
		env.setParallelism(k);

		//edges.partitionCustom(new DbhPartitioner<>(new CustomKeySelector(0),k), new CustomKeySelector<>(0)).writeAsCsv(outputPath, FileSystem.WriteMode.OVERWRITE).setParallelism(k);

		GraphStream<Long, NullValue, NullValue> graph = new SimpleEdgeStream<>(edges.partitionCustom(new DbhPartitioner<>(new CustomKeySelector(0),k), new CustomKeySelector<>(0)),env);


		//GraphStream<Long, NullValue, NullValue> graph = new SimpleEdgeStream<>(edges,env);

		//DataStream<DisjointSet<Long>> cc2 = graph.aggregate(new <Long, NullValue>(10));

	//7	edges.partitionCustom(new DbhPartitioner<>(new CustomKeySelector(0),k), new CustomKeySelector<>(0)).writeAsCsv(outputPath, FileSystem.WriteMode.OVERWRITE).setParallelism(k);




	}
	private static String InputPath = null;
	private static String outputPath = null;
	private static String log = null;
	private static int k = 0;
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



	///////code for partitioner/////////
	private static class DbhPartitioner<T> implements Partitioner<T> {
		private static final long serialVersionUID = 1L;
		CustomKeySelector keySelector;

		private int k;
		StoredState currentState;
		private static final int MAX_SHRINK = 100;
		private double seed;
		private int shrink;

		public DbhPartitioner(CustomKeySelector keySelector, int k)
		{
			this.keySelector = keySelector;
			this.k= k;
			this.currentState = new StoredState(k);
			seed = Math.random();
			Random r = new Random();
			shrink = r.nextInt(MAX_SHRINK);

		}

		@Override
		public int partition(Object key, int numPartitions) {

			long target = 0L;
			try {
				target = (long) keySelector.getValue(key);
			} catch (Exception e) {
				e.printStackTrace();
			}

			long source = (long) key;


			int machine_id = -1;

			StoredObject first_vertex = currentState.getRecord(source);
			StoredObject second_vertex = currentState.getRecord(target);


			int shard_u = Math.abs((int) ( (int) source*seed*shrink) % k);
			int shard_v = Math.abs((int) ( (int) target*seed*shrink) % k);

			int degree_u = first_vertex.getDegree() +1;
			int degree_v = second_vertex.getDegree() +1;

			if (degree_v<degree_u){
				machine_id = shard_v;
			}
			else if (degree_u<degree_v){
				machine_id = shard_u;
			}
			else{ //RANDOM CHOICE
				//*** PICK A RANDOM ELEMENT FROM CANDIDATES
				Random r = new Random();
				int choice = r.nextInt(2);
				if (choice == 0){
					machine_id = shard_u;
				}
				else if (choice == 1){
					machine_id = shard_v;
				}
				else{
					System.out.println("ERROR IN RANDOM CHOICE DBH");
					System.exit(-1);
				}
			}
			//UPDATE EDGES
			Edge e = new Edge<>(source, target, NullValue.getInstance());
			currentState.incrementMachineLoad(machine_id,e);

			//UPDATE RECORDS
			if (currentState.getClass() == StoredState.class){
				StoredState cord_state = (StoredState) currentState;
				//NEW UPDATE RECORDS RULE TO UPFDATE THE SIZE OF THE PARTITIONS EXPRESSED AS THE NUMBER OF VERTICES THEY CONTAINS
				if (!first_vertex.hasReplicaInPartition(machine_id)){ first_vertex.addPartition(machine_id); cord_state.incrementMachineLoadVertices(machine_id);}
				if (!second_vertex.hasReplicaInPartition(machine_id)){ second_vertex.addPartition(machine_id); cord_state.incrementMachineLoadVertices(machine_id);}
			}
			else{
				//1-UPDATE RECORDS
				if (!first_vertex.hasReplicaInPartition(machine_id)){ first_vertex.addPartition(machine_id);}
				if (!second_vertex.hasReplicaInPartition(machine_id)){ second_vertex.addPartition(machine_id);}
			}

			//3-UPDATE DEGREES

			//System.out.print("source"+source);
			//System.out.println("target"+target);
			//System.out.println("machineid"+machine_id);
			first_vertex.incrementDegree();
			second_vertex.incrementDegree();

			return machine_id;
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
				//	File file = new File("/Users/zainababbas/partitioning/gelly-streaming/count");

				// if file doesnt exists, then create it
				File file = new File("/Users/zainababbas/partitioning/gelly-streaming/count");

				// if file doesnt exists, then create it
				if (!file.exists()) {
					file.createNewFile();
				}

				FileWriter fw = new FileWriter(file.getAbsoluteFile());
				BufferedWriter bw = new BufferedWriter(fw);


				bw.write(count);
				bw.write("\n");

				bw.close();



				System.out.println(count+"mycount");
				if (count1 <= count2) {
					s2.merge(s1);
					return s2;
				}
				s1.merge(s2);
				return s1;
			}
		}
	}

}

