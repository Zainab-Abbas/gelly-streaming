package org.apache.flink.graph.streaming.partitioner;

/**
 * Created by zainababbas on 27/04/16.
 */

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class FennelCustom {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Tuple2<Long, List<Long>>> vertices = getGraphStream(env);
		vertices.getTransformation().getOutputType();
		vertices.partitionCustom(new Fennel(new SampleKeySelector(0), 6, 4), new SampleKeySelector(0)).print();

		env.execute("testing custom partitioner");
		System.out.println("lala");
	}

	/////key selector /////////////////

	private static DataStream<Tuple2<Long, List<Long>>> getGraphStream(StreamExecutionEnvironment env) {
		return env.fromCollection(getVertices());
	}


	///////code for partitioner/////////

	public static final List<Tuple2<Long, List<Long>>> getVertices() {
		List<Tuple2<Long, List<Long>>> vertices = new ArrayList<>();
		List<Long> n1 = new ArrayList<>();
		n1.add(4L);
		vertices.add(new Tuple2<>(1L, n1));
		List<Long> n2 = new ArrayList<>();
		n2.add(0, 3L);
		n2.add(0, 6L);
		vertices.add(new Tuple2<>(2L, n2));
		List<Long> n3 = new ArrayList<>();
		n3.add(0, 2L);
		vertices.add(new Tuple2<>(3L, n3));
		List<Long> n4 = new ArrayList<>();
		n4.add(0, 1L);
		n4.add(1, 5L);
		vertices.add(new Tuple2<>(4L, n4));
		List<Long> n5 = new ArrayList<>();
		n5.add(0, 4L);
		vertices.add(new Tuple2<>(5L, n5));
		List<Long> n6 = new ArrayList<>();
		n6.add(0, 2L);
		vertices.add(new Tuple2<>(6L, n6));
		return vertices;
	}

	private static class SampleKeySelector<K, EV> implements KeySelector<Tuple2<K, List<EV>>, K> {
		private final int key1;
		private List<EV> key2;
		private static final HashMap<Long, List<Long>> DoubleKey = new HashMap<>();

		public SampleKeySelector(int k) {
			this.key1 = k;
		}

		public K getKey(Tuple2<K, List<EV>> vertices) throws Exception {
			DoubleKey.put(vertices.getField(key1), vertices.getField(key1 + 1));
			return vertices.getField(key1);
		}

		public List<EV> getValue(Object k) throws Exception {
			key2 = (List<EV>) DoubleKey.get((long) k);
			DoubleKey.clear();
			return key2;
		}
	}

	private static class Fennel<K, EV, T> implements Partitioner<T> {
		private static final long serialVersionUID = 1L;
		private final HashMap<Long, List<Long>> Result = new HashMap<>();//partitionid, list of vertices placed
		private final List<Double> load = new ArrayList<>(); //for load of each partiton
		SampleKeySelector<T, ?> keySelector;
		private Long k;  //no. of partitions
		private double alpha = 0;  //parameters for formula
		private double gamma = 0;
		private double loadlimit = 0.0;     //k*v+n/n
		private int n = 0;        // no of nodes
		private int m = 0;        //no. of vertices

		public Fennel(SampleKeySelector<T, ?> keySelector, int n, int m) {
			this.keySelector = keySelector;
			this.k = (long) 4;
			this.n = n;
			this.m = m;
			this.alpha = (((Math.pow(k, 0.5)) * Math.pow(n, 1.5)) + m) / Math.pow(n, 1.5);
			this.gamma = 1.5;
			this.loadlimit = (k * 1.1 + n) / k;
		}

		@Override
		public int partition(Object key, int numPartitions) {

			List<Long> neighbours = new ArrayList<>();
			try {
				neighbours = (List<Long>) keySelector.getValue(key);
				System.out.println(neighbours);
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


			if (Result.isEmpty()) {
				for (int j = 0; j < k; j++) {
					load.add(j, 0.0);
				}
				load.set(0, 1.1);
				List<Long> L = new ArrayList<>();
				L.add(source);
				Result.put((long) 0, L);
				h = 0;

			} else {
				List<Double> num = new ArrayList<>();
				int n = 0;
				for (int j = 0; j < k; j++) {
					num.add(j, 0.0);
				}
				for (int i = 0; i < k; i++) {
					n = getValue(i, neighbours);
					num.set(i, (double) ((double) n - alpha * gamma * Math.pow(load.get(i), gamma - 1)));

				}

				Double first = 0.0;
				Double l = 0.0;
				int index1 = 0;
				first = num.get(0);
				for (int i = 1; i < k; i++) {
					if (first.compareTo(num.get(i)) < 0 && load.get(i).compareTo(loadlimit) < 0) {

						first = num.get(i);
						index1 = i;

					}
				}

				h = index1;
				l = load.get(index1);
				l = l + 1;
				load.set(index1, l);
				if (Result.get((long) index1) == null) {
					List<Long> L = new ArrayList<>();
					L.add(source);
					Result.put((long) index1, L);
				} else {
					List<Long> L;
					L = Result.get((long) index1);
					L.add(source);
					Result.put((long) index1, L);
				}
			}
			return h;
		}

		public int getValue(int p, List<Long> n) {

			int ne = 0;
			List<Long> list;
			for (int i = 0; i < n.size(); i++) {
				Long v = n.get(i);
				list = Result.get((long) p);
				if (list != null) {
					if (list.contains(v)) {
						ne++;
					}
				}
			}

			return ne;
		}


	}


}

