package org.apache.flink.graph.streaming.partitioner;

/**
 * Created by zainababbas on 27/04/16.
 */

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.hadoop.shaded.com.google.common.collect.HashBasedTable;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Table;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;

import java.util.*;


import java.util.ArrayList;
import java.util.List;


public class NeighbourhoodCustom {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Edge<Long, NullValue>> edges = getGraphStream(env);
		edges.getTransformation().getOutputType();
		edges.partitionCustom(new test(new SampleKeySelector(0)),new SampleKeySelector(0)).print();

		env.execute("testing custom partitioner");
		System.out.println("lala");
	}

	private static class SampleKeySelector<K, EV> implements KeySelector<Edge<K, EV>, K> {
		private final int key;
		private static final HashMap<Long,Long> DoubleKey = new HashMap<>();

		public SampleKeySelector(int k) {
			this.key = k;
		}

		public K getKey(Edge<K, EV> edge) throws Exception {
			DoubleKey.put(edge.getField(key),edge.getField(key+1));
			return edge.getField(key);
		}

		public long getValue (Object k) throws Exception {
			return DoubleKey.get((long) k);
		}
	}


	///////code for partitioner/////////
	private static class test<K, EV, T> implements Partitioner<T> {
		private static final long serialVersionUID = 1L;
		SampleKeySelector<T, ?> keySelector;
		private final Table<Long, Long, Long> Degree = HashBasedTable.create();   //for <partition.no, vertexId, Degree>
		private final HashMap<Long, List<Tuple2<Long, Long>>> Result = new HashMap<>();
		private final List<Long> load = new ArrayList<>(); //for load of each partiton
		private final List<Long> subset = new ArrayList<>();
		private Long k;   //no. of partitions

		public test(SampleKeySelector<T, ?> keySelector) {
			this.keySelector = keySelector;
			this.k = (long) 4;

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


			if (Degree.isEmpty()) {
				for (int j = 0; j < k; j++) {
					load.add(j, (long) 0);
				}
				load.set(0, (long) 1);
				Degree.put((long) 0, source, (long) 1);
				Degree.put((long) 0, target, (long) 1);
				List<Tuple2<Long, Long>> L = new ArrayList<>();
				L.add(new Tuple2<>(source, target));
				Result.put((long) 0, L);
				h = 0;

			} else {
				//condition 1 both vertices in same partition
				for (int i = 0; i < k; i++) {

					int value = 0;
					value = getValue(source, target, i);
					subset.add(i, (long) value);

					//get the value of S(k) from each partition using return
				}

				h = Cost(source, target);
				System.out.println("one");
				subset.clear();
			}

			return h;
		}

		public int Cost(Long source, Long target) {
			Long max = subset.get(0);
			int sub = 0;
			List<Integer> tie = new ArrayList<>();
			for (int j = 1; j < k; j++) {

				if (max < subset.get(j)) {
					max = subset.get(j);
					sub = j;
				} else if (max == subset.get(j)) {
					if (!tie.contains(sub)) {
						tie.add(sub);
					}
					if (!tie.contains(j)) {
						tie.add(j);
					}

				}
			}

			int h = addEdge(tie, sub, source, target);
			return h;


		}


		public int addEdge(List<Integer> Tie, int Max, Long source, Long target) {

			if (!Tie.isEmpty()) {
				int max = Tie.get(0);
				int s = Tie.size();
				int val_sub = 0;
				int val = (int) (subset.get(max) + load.get(max));
				for (int j = 1; j <= (s - 1); j++) {

					if (val >= (int) (subset.get(j) + load.get(j))) {
						val = (int) (subset.get(j) + load.get(j));
						val_sub = j;
						System.out.println("Hola   1!!");
					}
				}

				Long d1 = Degree.get((long) val_sub, source);
				Long d2 = Degree.get((long) val_sub, target);
				if (d1 == null) {
					d1 = (long) 0;
				}
				if (d2 == null) {
					d2 = (long) 0;
				}       //return 2 as 2 vertices here
				d1++;
				d2++;
				Long l = load.get(val_sub);
				l++;
				load.set(val_sub, l);
				Degree.put((long) val_sub, source, d1);
				Degree.put((long) val_sub, target, d2);
				if (Result.get((long) val_sub) != null) {
					List<Tuple2<Long, Long>> L = Result.get((long) val_sub);
					L.add(new Tuple2<>(source, target));
					Result.put((long) val_sub, L);
				} else {
					List<Tuple2<Long, Long>> L = new ArrayList<>();
					L.add(new Tuple2<>(source, target));
					Result.put((long) val_sub, L);
				}
				return val_sub;
			} else {
				System.out.println("Hola   2 !!");
				Long d1 = Degree.get((long) Max, source);
				Long d2 = Degree.get((long) Max, target);
				if (d1 == null) {
					d1 = (long) 0;
				}
				if (d2 == null) {
					d2 = (long) 0;
				}       //return 2 as 2 vertices here
				d1++;
				d2++;
				Long l = load.get(Max);
				l++;
				load.set(Max, l);
				Degree.put((long) Max, source, d1);
				Degree.put((long) Max, target, d2);
				if (Result.get((long) Max) != null) {
					List<Tuple2<Long, Long>> L = Result.get((long) Max);
					L.add(new Tuple2<>(source, target));
					Result.put((long) Max, L);
				} else {
					List<Tuple2<Long, Long>> L = new ArrayList<>();
					L.add(new Tuple2<>(source, target));
					Result.put((long) Max, L);
				}

				return Max;
			}

		}

		public int getValue(Long source, Long target, int p) {
			{
				int i = 0;
				Long V1 = source;
				Long V2 = target;
				if (Degree.contains((long) p, (long) V1) && Degree.contains((long) p, (long) V2)) {

					System.out.println("one");
					i = 2;
				} else if (Degree.contains((long) p, (long) V1) && !Degree.contains((long) p, (long) V2)) {

					System.out.println("one");
					i = 1;
				} else if (!Degree.contains((long) p, (long) V1) && Degree.contains((long) p, (long) V2)) {
					System.out.println("one");
					i = 1;
				} else {
					i = 0;
				}
				return i;
			}

		}
	}
		private static DataStream<Edge<Long, NullValue>> getGraphStream(StreamExecutionEnvironment env) {
			return env.fromCollection(getEdges());
		}

		public static final List<Edge<Long, NullValue>> getEdges() {
			List<Edge<Long, NullValue>> edges = new ArrayList<>();
			edges.add(new Edge<>(1L, 2L, NullValue.getInstance()));
			edges.add(new Edge<>(1L, 3L, NullValue.getInstance()));
			edges.add(new Edge<>(2L, 3L, NullValue.getInstance()));
			edges.add(new Edge<>(4L, 0L, NullValue.getInstance()));
			edges.add(new Edge<>(6L, 7L, NullValue.getInstance()));
			edges.add(new Edge<>(8L, 9L, NullValue.getInstance()));
			return edges;
		}

}

