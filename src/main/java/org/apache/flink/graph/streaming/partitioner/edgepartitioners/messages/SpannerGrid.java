package org.apache.flink.graph.streaming.partitioner.edgepartitioners.messages;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.EdgesFold;
import org.apache.flink.graph.streaming.GraphStream;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.graph.streaming.WindowGraphAggregation;
import org.apache.flink.graph.streaming.library.Spanner;
import org.apache.flink.graph.streaming.partitioner.edgepartitioners.keyselector.CustomKeySelector;
import org.apache.flink.graph.streaming.partitioner.object.StoredObject;
import org.apache.flink.graph.streaming.partitioner.object.StoredState;
import org.apache.flink.graph.streaming.summaries.AdjacencyListGraph;
import org.apache.flink.graph.streaming.summaries.DisjointSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.io.*;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.TimeUnit;


/**
 * Created by zainababbas on 07/02/2017.
 */
public class SpannerGrid {

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);


		DataStream<Edge<Long, NullValue>> edges = getGraphStream(env);
		env.setParallelism(k);

		//edges.partitionCustom(new DbhPartitioner<>(new CustomKeySelector(0),k), new CustomKeySelector<>(0)).writeAsCsv(outputPath, FileSystem.WriteMode.OVERWRITE).setParallelism(k);

		GraphStream<Long, NullValue, NullValue> graph = new SimpleEdgeStream<>(edges.partitionCustom(new GridPartitioner<>(new CustomKeySelector(0),k), new CustomKeySelector<>(0)),env);


		//GraphStream<Long, NullValue, NullValue> graph = new SimpleEdgeStream<>(edges,env);
		System.out.print(env.getParallelism());
		DataStream<AdjacencyListGraph<Long>> spanner =graph.aggregate(new Spanner<Long, NullValue>(50000, k));

		//	edges.partitionCustom(new DbhPartitioner<>(new CustomKeySelector(0),k), new CustomKeySelector<>(0)).writeAsCsv(outputPath, FileSystem.WriteMode.OVERWRITE).setParallelism(k);
		spanner.flatMap(new FlattenSet())
				.keyBy(0).timeWindow(Time.of(50000, TimeUnit.MILLISECONDS))
				.fold(new Tuple2<>(0l, 0l), new IdentityFold());


		JobExecutionResult result = env.execute("My Flink Job");


		try {
			FileWriter fw = new FileWriter(log, true); //the true will append the new data
			fw.write("The job took " + result.getNetRuntime(TimeUnit.SECONDS) + " seconds to execute" + "\n");//appends the string to the file
			fw.write("The job took " + result.getNetRuntime(TimeUnit.NANOSECONDS) + " nanoseconds to execute" + "\n");
			fw.close();
		} catch (IOException ioe) {
			System.err.println("IOException: " + ioe.getMessage());
		}

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


	public static final class FlattenSet implements FlatMapFunction<AdjacencyListGraph<Long>, Tuple2<Long, Long>> {

		private Tuple2<Long, Long> t = new Tuple2<>();

		@Override
		public void flatMap(AdjacencyListGraph<Long> g, Collector<Tuple2<Long, Long>> out) {
			for (Long src : g.getAdjacencyMap().keySet()) {
				t.setField(src, 0);
				for (Long trg : g.getAdjacencyMap().get(src)) {
					t.setField(trg, 1);
					out.collect(t);
				}
			}
		}
	}

	@SuppressWarnings("serial")
	public static final class IdentityFold implements FoldFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
		public Tuple2<Long, Long> fold(Tuple2<Long, Long> accumulator, Tuple2<Long, Long> value) throws Exception {
			return value;
		}
	}
	///////code for partitioner/////////
	private static class HDRF<T> implements Partitioner<T> {
		private static final long serialVersionUID = 1L;
		CustomKeySelector keySelector;
		private int epsilon = 1;
		private double lamda;
		private StoredState currentState;
		private int k = 0;

		public HDRF(CustomKeySelector keySelector, int k ,double lamda) {
			this.keySelector = keySelector;
			this.currentState = new StoredState(k);
			this.lamda = lamda;
			this.k=k;


		}

		@Override
		public int partition(Object key,  int numPartitions) {

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

			int min_load = currentState.getMinLoad();
			int max_load = currentState.getMaxLoad();

			LinkedList<Integer> candidates = new LinkedList<Integer>();
			double MAX_SCORE = 0;

			for (int m = 0; m < k; m++) {

				int degree_u = first_vertex.getDegree() + 1;
				int degree_v = second_vertex.getDegree() + 1;
				int SUM = degree_u + degree_v;
				double fu = 0;
				double fv = 0;
				if (first_vertex.hasReplicaInPartition(m)) {
					fu = degree_u;
					fu /= SUM;
					fu = 1 + (1 - fu);
				}
				if (second_vertex.hasReplicaInPartition(m)) {
					fv = degree_v;
					fv /= SUM;
					fv = 1 + (1 - fv);
				}
				int load = currentState.getMachineLoad(m);
				double bal = (max_load - load);
				bal /= (epsilon + max_load - min_load);
				if (bal < 0) {
					bal = 0;
				}
				double SCORE_m = fu + fv + lamda * bal;
				if (SCORE_m < 0) {
					System.out.println("ERRORE: SCORE_m<0");
					System.out.println("fu: " + fu);
					System.out.println("fv: " + fv);
					System.out.println("GLOBALS.LAMBDA: " + lamda);
					System.out.println("bal: " + bal);
					System.exit(-1);
				}
				if (SCORE_m > MAX_SCORE) {
					MAX_SCORE = SCORE_m;
					candidates.clear();
					candidates.add(m);
				} else if (SCORE_m == MAX_SCORE) {
					candidates.add(m);
				}
			}


			if (candidates.isEmpty()) {
				System.out.println("ERROR: GreedyObjectiveFunction.performStep -> candidates.isEmpty()");
				System.out.println("MAX_SCORE: " + MAX_SCORE);
				System.exit(-1);
			}

			//*** PICK A RANDOM ELEMENT FROM CANDIDATES
			Random r = new Random();
			int choice = r.nextInt(candidates.size());
			machine_id = candidates.get(choice);


			if (currentState.getClass() == StoredState.class) {
				StoredState cord_state = (StoredState) currentState;
				//NEW UPDATE RECORDS RULE TO UPFDATE THE SIZE OF THE PARTITIONS EXPRESSED AS THE NUMBER OF VERTICES THEY CONTAINS
				if (!first_vertex.hasReplicaInPartition(machine_id)) {
					first_vertex.addPartition(machine_id);
					cord_state.incrementMachineLoadVertices(machine_id);
				}
				if (!second_vertex.hasReplicaInPartition(machine_id)) {
					second_vertex.addPartition(machine_id);
					cord_state.incrementMachineLoadVertices(machine_id);
				}
			} else {
				//1-UPDATE RECORDS
				if (!first_vertex.hasReplicaInPartition(machine_id)) {
					first_vertex.addPartition(machine_id);
				}
				if (!second_vertex.hasReplicaInPartition(machine_id)) {
					second_vertex.addPartition(machine_id);
				}
			}

			Edge e = new Edge<>(source, target, NullValue.getInstance());
			//2-UPDATE EDGES
			currentState.incrementMachineLoad(machine_id, e);

			//3-UPDATE DEGREES
			first_vertex.incrementDegree();
			second_vertex.incrementDegree();
			//System.out.print("source" + source);
			//System.out.print(target);
			//System.out.println(machine_id);
				/*System.out.print("source"+source);
				System.out.println("target"+target);
				System.out.println("machineid"+machine_id);*/

			return machine_id;

		}
	}

	private static class GreedyPartitioner<T> implements Partitioner<T> {
		private static final long serialVersionUID = 1L;
		CustomKeySelector keySelector;
		private int epsilon = 1;

		private int k;
		StoredState currentState;

		public GreedyPartitioner(CustomKeySelector keySelector, int k)
		{
			this.keySelector = keySelector;
			this.k= k;
			this.currentState = new StoredState(k);

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

			int min_load = currentState.getMinLoad();
			int max_load = currentState.getMaxLoad();


			LinkedList<Integer> candidates = new LinkedList<Integer>();
			double MAX_SCORE = 0;
			for (int m = 0; m<k; m++){
				int sd = 0;
				int td = 0;
				if (first_vertex.hasReplicaInPartition(m)){ sd = 1;}
				if (second_vertex.hasReplicaInPartition(m)){ td = 1;}
				int load = currentState.getMachineLoad(m);

				//OLD BALANCE
				double bal = (max_load-load);
				bal /= (epsilon + max_load - min_load);
				if (bal<0){ bal = 0;}
				double SCORE_m = sd + td + bal;


				if (SCORE_m>MAX_SCORE){
					MAX_SCORE = SCORE_m;
					candidates.clear();
					candidates.add(m);
				}
				else if (SCORE_m==MAX_SCORE){
					candidates.add(m);
				}
			}

			//*** CHECK TO AVOID ERRORS
			if (candidates.isEmpty()){
				System.out.println("ERRORE: GreedyObjectiveFunction.performStep -> candidates.isEmpty()");
				System.out.println("MAX_SCORE: "+MAX_SCORE);
				System.exit(-1);
			}

			//*** PICK A RANDOM ELEMENT FROM CANDIDATES
			Random r = new Random();
			int choice = r.nextInt(candidates.size());
			machine_id = candidates.get(choice);
			//1-UPDATE RECORDS
			if (currentState.getClass() == StoredState.class){
				StoredState cord_state = (StoredState) currentState;
				//NEW UPDATE RECORDS RULE TO UPDATE THE SIZE OF THE PARTITIONS EXPRESSED AS THE NUMBER OF VERTICES THEY CONTAINS
				if (!first_vertex.hasReplicaInPartition(machine_id)){ first_vertex.addPartition(machine_id); cord_state.incrementMachineLoadVertices(machine_id);}
				if (!second_vertex.hasReplicaInPartition(machine_id)){ second_vertex.addPartition(machine_id); cord_state.incrementMachineLoadVertices(machine_id);}
			}
			else{
				//1-UPDATE RECORDS
				if (!first_vertex.hasReplicaInPartition(machine_id)){ first_vertex.addPartition(machine_id); }
				if (!second_vertex.hasReplicaInPartition(machine_id)){ second_vertex.addPartition(machine_id); }
			}

			//2-UPDATE EDGES

			Edge e = new Edge<>(source, target, NullValue.getInstance());
			currentState.incrementMachineLoad(machine_id, e);

			/*System.out.print("source"+source);
			System.out.println("target"+target);
			System.out.println("machineid"+machine_id);*/
			first_vertex.incrementDegree();
			second_vertex.incrementDegree();
			return machine_id;
		}



	}


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

	private static class GridPartitioner<T> implements Partitioner<T> {
		private static final long serialVersionUID = 1L;
		CustomKeySelector keySelector;

		private static final int MAX_SHRINK = 100;
		private double seed;
		private int shrink;
		private int k;
		private int nrows, ncols;
		LinkedList<Integer>[] constraint_graph;
		StoredState currentState;

		public GridPartitioner(CustomKeySelector keySelector, int k)
		{
			this.keySelector = keySelector;
			this.k= k;
			this.seed = Math.random();
			Random r = new Random();
			shrink = r.nextInt(MAX_SHRINK);
			this.constraint_graph = new LinkedList[k];
			this.currentState = new StoredState(k);

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


			make_grid_constraint();

			int machine_id = -1;

			StoredObject first_vertex = currentState.getRecord(source);
			StoredObject second_vertex = currentState.getRecord(target);


			int shard_u = Math.abs((int) ( (int) source*0.55*79) % k);
			int shard_v = Math.abs((int) ( (int) target*0.55*79) % k);

			LinkedList<Integer> costrained_set = (LinkedList<Integer>) constraint_graph[shard_u].clone();
			costrained_set.retainAll(constraint_graph[shard_v]);

			//CASE 1: GREEDY ASSIGNMENT
			LinkedList<Integer> candidates = new LinkedList<Integer>();
			int min_load = Integer.MAX_VALUE;
			for (int m : costrained_set){
				int load = currentState.getMachineLoad(m);
				if (load<min_load){
					candidates.clear();
					min_load = load;
					candidates.add(m);
				}
				if (load == min_load){
					candidates.add(m);
				}
			}
			//*** PICK A RANDOM ELEMENT FROM CANDIDATES
			Random r = new Random();
			int choice = r.nextInt(candidates.size());
			machine_id = candidates.get(choice);

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



			//System.out.print("source"+source);
			//System.out.println("target"+target);
			//System.out.println("machineid"+machine_id);

			return machine_id;
		}

		private void make_grid_constraint() {
			initializeRowColGrid();
			for (int i = 0; i < k; i++) {
				LinkedList<Integer> adjlist = new LinkedList<Integer>();
				// add self
				adjlist.add(i);
				// add the row of i
				int rowbegin = (i/ncols) * ncols;
				for (int j = rowbegin; j < rowbegin + ncols; ++j)
					if (i != j) adjlist.add(j);
				// add the col of i
				for (int j = i % ncols; j < k; j+=ncols){
					if (i != j) adjlist.add(j);
				}
				Collections.sort(adjlist);
				constraint_graph[i]=adjlist;
			}

		}

		private void initializeRowColGrid() {
			double approx_sqrt = Math.sqrt(k);
			nrows = (int) approx_sqrt;
			for (ncols = nrows; ncols <= nrows + 2; ++ncols) {
				if (ncols * nrows == k) {
					return;
				}
			}
			System.out.println("ERRORE Num partitions "+k+" cannot be used for grid ingress.");
			System.exit(-1);
		}





	}

	public static  DataStream<Edge<Long, NullValue>> getGraphStream(StreamExecutionEnvironment env) throws IOException {

		return env.readTextFile(InputPath)
					   .map(new MapFunction<String, Edge<Long, NullValue>>() {
						   @Override
						   public Edge<Long, NullValue> map(String s) throws Exception {
							   String[] fields = s.split("\\ ");
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

