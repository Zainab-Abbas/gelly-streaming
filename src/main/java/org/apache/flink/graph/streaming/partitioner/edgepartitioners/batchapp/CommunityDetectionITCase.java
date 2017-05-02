package org.apache.flink.graph.streaming.partitioner.edgepartitioners.batchapp;

/**
 * Created by zainababbas on 18/04/2017.
 */

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.CommunityDetection;
import org.apache.flink.graph.streaming.partitioner.edgepartitioners.keyselector.CustomKeySelector;

import java.util.List;
import java.util.Random;

public class CommunityDetectionITCase  {


	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Edge<Long, Double>> data = env.readTextFile("/Users/zainababbas/working/gelly-streaming/BIGGGGGG/app/appmovf").map(new MapFunction<String, Edge<Long, Double>>() {

			@Override
			public Edge<Long, Double> map(String s) {
				String[] fields = s.split("\\,");
				long src = Long.parseLong(fields[0]);
				long trg = Long.parseLong(fields[1]);
				return new Edge<>(src, trg, 1.0);
			}
		});


		Graph<Long, Long, Double> inputGraph = Graph.fromDataSet(
				data.partitionCustom(new HashPartitioner<>(new CustomKeySelector(0)), new CustomKeySelector<>(0)), new InitLabels(), env);

		List<Vertex<Long, Long>> result = inputGraph.run(new CommunityDetection<Long>(1, 0.5))
												  .getVertices().collect();


		Object[] arr =result.toArray();
		for(int i=0; i<arr.length;i++)
		{
			Object v= arr[i];

			System.out.println(v.toString());
		}

	}

	private static class HashPartitioner<T> implements Partitioner<T> {
		private static final long serialVersionUID = 1L;
		CustomKeySelector keySelector;
		private static final int MAX_SHRINK = 100;
		private double seed;
		private int shrink;
		public HashPartitioner(CustomKeySelector keySelector)
		{
			this.keySelector = keySelector;
			System.out.println("createdsfsdfsdfsdf");
			this.seed = Math.random();
			Random r = new Random();
			shrink = r.nextInt(MAX_SHRINK);

		}

		@Override
		public int partition(Object key, int numPartitions) {
			//return MathUtils.murmurHash(key.hashCode()) % numPartitions;
			return Math.abs((int) (  (int) Integer.parseInt(key.toString())*seed*shrink) % numPartitions);


		}

	}
	@SuppressWarnings("serial")
	private static final class InitLabels implements MapFunction<Long, Long> {

		public Long map(Long id) {
			return id;
		}
	}
}
