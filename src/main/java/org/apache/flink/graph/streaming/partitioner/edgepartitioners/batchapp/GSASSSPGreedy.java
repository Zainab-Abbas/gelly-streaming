package org.apache.flink.graph.streaming.partitioner.edgepartitioners.batchapp;

/**
 * Created by zainababbas on 18/04/2017.
 */

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.Neighbor;
import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.graph.streaming.partitioner.edgepartitioners.keyselector.CustomKeySelector2;
import org.apache.flink.graph.streaming.partitioner.object.StoredObject;
import org.apache.flink.graph.streaming.partitioner.object.StoredState;
import org.apache.flink.types.NullValue;

import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class GSASSSPGreedy implements ProgramDescription {

	// --------------------------------------------------------------------------------------------
	//  Program
	// --------------------------------------------------------------------------------------------

	public static void main(String[] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();



		DataSet<Edge<Long, NullValue>> data = env.readTextFile(edgesInputPath).map(new MapFunction<String, Edge<Long, NullValue>>() {

			@Override
			public Edge<Long, NullValue> map(String s) {
				String[] fields = s.split("\\ ");
				long src = Long.parseLong(fields[0]);
				long trg = Long.parseLong(fields[1]);
				return new Edge<>(src, trg, NullValue.getInstance());
			}
		});

        env.setParallelism(k);
	//DataSet<Edge<Long, NullValue>> partitionedData =
	//			data.partitionCustom(new GreedyPartitioner<>(new CustomKeySelector2(0),k), new CustomKeySelector2<>(0));

		Graph<Long, Double, NullValue> graph = Graph.fromDataSet(data.partitionCustom(new GreedyPartitioner<>(new CustomKeySelector2(0),k), new CustomKeySelector2<>(0)), new InitVertices(srcVertexId), env);
		//Graph<Long, Double, NullValue> graph = Graph.fromDataSet(data, new InitVertices(srcVertexId), env);

		// Execute the GSA iteration
		Graph<Long, Double, NullValue> result = graph.runGatherSumApplyIteration(
				new CalculateDistances(), new ChooseMinDistance(), new UpdateDistance(), maxIterations);

		// Extract the vertices as the result
		DataSet<Vertex<Long, Double>> singleSourceShortestPaths = result.getVertices();

		if(fileOutput) {
			singleSourceShortestPaths.writeAsCsv(outputPath, "\n", ",");

			// since file sinks are lazy, we trigger the execution explicitly
		} else {
			singleSourceShortestPaths.print();
		}

		JobExecutionResult result1 = env.execute("My Flink Job1");

		try {
			FileWriter fw = new FileWriter(logPath, true); //the true will append the new data
			//fw.write("The job took " + result.getNetRuntime(TimeUnit.SECONDS) + " seconds to execute" + "\n");//appends the string to the file
			//fw.write("The job took " + result.getNetRuntime(TimeUnit.NANOSECONDS) + " nanoseconds to execute" + "\n");
			fw.write("The job1 took " + result1.getNetRuntime(TimeUnit.SECONDS) + " seconds to execute" + "\n");//appends the string to the file
			fw.write("The job1 took " + result1.getNetRuntime(TimeUnit.NANOSECONDS) + " nanoseconds to execute" + "\n");
			fw.close();
		} catch (IOException ioe) {
			System.err.println("IOException: " + ioe.getMessage());
		}





	}

	// --------------------------------------------------------------------------------------------
	//  Single Source Shortest Path UDFs
	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("serial")
	private static final class InitVertices implements MapFunction<Long, Double>{

		private long srcId;

		public InitVertices(long srcId) {
			this.srcId = srcId;
		}

		public Double map(Long id) {
			if (id.equals(srcId)) {
				return 0.0;
			}
			else {
				return Double.POSITIVE_INFINITY;
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class CalculateDistances extends GatherFunction<Double, NullValue, Double> {

		public Double gather(Neighbor<Double,  NullValue> neighbor) {
			return neighbor.getNeighborValue() + 1;
		}

	}

	@SuppressWarnings("serial")
	private static final class ChooseMinDistance extends SumFunction<Double, NullValue, Double> {

		public Double sum(Double newValue, Double currentValue) {
			return Math.min(newValue, currentValue);
		}
	}

	@SuppressWarnings("serial")
	private static final class UpdateDistance extends ApplyFunction<Long, Double, Double> {

		public void apply(Double newDistance, Double oldDistance) {
			if (newDistance < oldDistance) {
				setResult(newDistance);
			}
		}

	}

	// --------------------------------------------------------------------------------------------
	//  Util methods
	// --------------------------------------------------------------------------------------------
	private static class GreedyPartitioner<T> implements Partitioner<T> {
		private static final long serialVersionUID = 1L;
		CustomKeySelector2 keySelector;
		private int epsilon = 1;
		static int count =0;
		private int k;
		StoredState currentState;

		public GreedyPartitioner(CustomKeySelector2 keySelector, int k)
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

				count++;
				System.out.println(count);
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

	private static boolean fileOutput = false;

	private static Long srcVertexId = 1l;

	private static String edgesInputPath = null;

	private static String outputPath = null;

	private static String logPath = null;

	private static int maxIterations = 5;

	private static int k = 4;

	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			if(args.length != 6) {
				System.err.println("Usage: GSASSSPHash <source vertex id>" +
										   " <input edges path> <output path> <log>  <num iterations> <no. of partitions>");
				return false;
			}

			fileOutput = true;
			srcVertexId = Long.parseLong(args[0]);
			edgesInputPath = args[1];
			outputPath = args[2];
			logPath = args[3];
			maxIterations = Integer.parseInt(args[4]);
			k = Integer.parseInt(args[5]);
		} else {
			System.out.println("Executing GSASingle Source Shortest Paths example "
									   + "with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("Usage: GSASSSPHash <source vertex id>" +
									   " <input edges path> <output path> <log path><num iterations> <no. of partitions>");
		}
		return true;
	}

	@Override
	public String getDescription() {
		return "GSA Single Source Shortest Paths";
	}

}

