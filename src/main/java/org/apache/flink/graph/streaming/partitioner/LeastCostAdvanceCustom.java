package org.apache.flink.graph.streaming.partitioner;

/**
 * Created by zainababbas on 27/04/16.
 */

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class LeastCostAdvanceCustom {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Edge<Long, NullValue>> edges = getGraphStream(env);
		edges.getTransformation().getOutputType();
		edges.partitionCustom(new test(new SampleKeySelector(0), 9, 6), new SampleKeySelector(0)).print();

		env.execute("testing custom partitioner");
		System.out.println("lala");
	}

	private static class SampleKeySelector<K, EV> implements KeySelector<Edge<K, EV>, K> {
		private final int key;
		private static final HashMap<Long, Long> DoubleKey = new HashMap<>();

		public SampleKeySelector(int k) {
			this.key = k;
		}

		public K getKey(Edge<K, EV> edge) throws Exception {
			DoubleKey.put(edge.getField(key), edge.getField(key + 1));
			return edge.getField(key);
		}

		public long getValue(Object k) throws Exception {
			return DoubleKey.get((long) k);
		}
	}


	///////code for partitioner/////////
	private static class test<K, EV, T> implements Partitioner<T> {
		private static final long serialVersionUID = 1L;
		SampleKeySelector<T, ?> keySelector;
		int n;  // no. of nodes
		int m; // no. of edges
		private final HashMap<Long, List<Long>> vertices = new HashMap<>();  //for <partition.no, vertexId>
		private final List<Long> load = new ArrayList<>(); //for load of each partiton
		private final List<Double> cost = new ArrayList<>();
		private Long k;   //no. of partitions

		private double alpha = 0;  //parameters for formula
		private double gamma = 0;

		public test(SampleKeySelector<T, ?> keySelector, int n, int m) {
			this.keySelector = keySelector;
			this.k = (long) 4;
			this.n = n;
			this.m = m;
			this.alpha = (m * Math.pow(n, 1.5)) + ((Math.pow(k, 0.5))) / Math.pow(n, 1.5);
			this.gamma = 1.5;

		}

		@Override
		public int partition(Object key, int numPartitions) {

			long target = 0;
			try {
				target = keySelector.getValue(key);
				System.out.println(target);
			} catch (Exception e) {
				e.printStackTrace();
			}

			long source = (long) key;
			System.out.println("source");
			System.out.println(source);
			System.out.println("target");
			System.out.println();
			System.out.println("end");

			int h = 0;

			if (vertices.isEmpty()) {
				for (int j = 0; j < k; j++) {
					load.add(j, (long) 0);
				}
				load.set(0, (long) 1);
				List<Long> L = new ArrayList<>();
				L.add(source);
				L.add(target);
				vertices.put((long) 0, L);
				h = 0;

			} else {
				//condition 1 both vertices in same partition
				for (int j = 0; j < k; j++) {
					cost.add(j, (double) 0);
				}
				for (int i = 0; i < k; i++) {

					int c = 0;
					c = getValue(source, target, i);
					cost.set(i, (double) ((double) c - alpha * gamma * Math.pow(load.get(i), gamma - 1)));

					//get the value of S(k) from each partition using return
				}

				h = CompareCost();

				Long l = load.get(h);
				l++;
				load.set(h, l);
				if (vertices.get((long) h) != null) {
					List<Long> L = vertices.get((long) h);
					if (!L.contains(source)) {
						L.add(source);
					}
					if (!L.contains(target)) {
						L.add(target);
					}
					vertices.put((long) h, L);
				} else {
					List<Long> L = new ArrayList<>();
					L.add(source);
					L.add(target);
					vertices.put((long) h, L);
				}
				System.out.println("one");
				cost.clear();
			}

			return h;
		}

		public int CompareCost() {
			Double max = cost.get(0);
			int sub = 0;
			for (int j = 1; j < k; j++) {

				if (max < cost.get(j)) {
					max = cost.get(j);
					sub = j;
				}
			}
			return sub;
		}

		public int getValue(Long source, Long target, int p) {
			int i = 0;
			List<Long> L;
			L = vertices.get((long) p);
			if (L != null) {
				if (L.contains(source) && L.contains(target)) {

					System.out.println("one");
					i = 2;
				} else if (L.contains(source) && !L.contains(target)) {

					System.out.println("one");
					i = 1;
				} else if (!L.contains(source) && L.contains(target)) {
					System.out.println("one");
					i = 1;
				} else {
					i = 0;
				}

			}
			return i;
		}


	}

	private static DataStream<Edge<Long, NullValue>> getGraphStream(StreamExecutionEnvironment env) {
		return env.fromCollection(getEdges());
	}

	public static final List<Edge<Long, NullValue>> getEdges() {
		List<Edge<Long, NullValue>> edges = new ArrayList<>();
		edges.add(new Edge<>(1L, 2L, NullValue.getInstance()));
		edges.add(new Edge<>(1L, 2L, NullValue.getInstance()));
		edges.add(new Edge<>(4L, 0L, NullValue.getInstance()));
		edges.add(new Edge<>(6L, 7L, NullValue.getInstance()));
		edges.add(new Edge<>(8L, 9L, NullValue.getInstance()));
		edges.add(new Edge<>(4L, 5L, NullValue.getInstance()));
		return edges;
	}


}

