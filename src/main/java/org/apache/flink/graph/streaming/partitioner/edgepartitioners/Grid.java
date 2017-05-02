package org.apache.flink.graph.streaming.partitioner.edgepartitioners;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.partitioner.edgepartitioners.keyselector.CustomKeySelector;
import org.apache.flink.graph.streaming.partitioner.object.StoredObject;
import org.apache.flink.graph.streaming.partitioner.object.StoredState;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by zainababbas on 07/02/2017.
 */
public class Grid {

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStream<Edge<Long, NullValue>> edges = getGraphStream(env);

		edges.partitionCustom(new GridPartitioner<>(new CustomKeySelector(0),k), new CustomKeySelector<>(0)).writeAsCsv(outputPath, FileSystem.WriteMode.OVERWRITE).setParallelism(k);

		//edges.partitionCustom(new GridPartitioner<>(new CustomKeySelector(0),k), new CustomKeySelector<>(0))
		//		.addSink(new TimestampingSink(outputPath)).setParallelism(k);
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
				System.err.println("Usage: Grid <input edges path> <output path> <log> ");
				return false;
			}

			InputPath = args[0];
			outputPath = args[1];
			log = args[2];
			k = (int) Long.parseLong(args[3]);
		} else {
			System.out.println("Executing example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println(" Usage: Grid <input edges path> <output path> <log>");
		}
		return true;
	}



	///////code for partitioner/////////
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


			int shard_u = Math.abs((int) ( (int) source*seed*shrink) % k);
			int shard_v = Math.abs((int) ( (int) target*seed*shrink) % k);

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
							   String[] fields = s.split("\\,");
							   long src = Long.parseLong(fields[0]);
							   long trg = Long.parseLong(fields[1]);
							   return new Edge<>(src, trg, NullValue.getInstance());
						   }
					   });

	}

}
