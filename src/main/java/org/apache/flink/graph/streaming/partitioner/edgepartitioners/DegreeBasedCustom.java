package org.apache.flink.graph.streaming.partitioner.edgepartitioners;

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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class DegreeBasedCustom {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Edge<Long, NullValue>> edges = getGraphStream(env);
		edges.partitionCustom(new DegreeBased(new SampleKeySelector(0),12), new SampleKeySelector(0)).print();
		env.execute("testing custom partitioner");
		System.out.println("lala");
	}

	private static class SampleKeySelector<K, EV> implements KeySelector<Edge<K, EV>, K> {
		private final int key1;
		private EV key2;
		private static final HashMap<Object,Object> KeyMap = new HashMap<>();

		public SampleKeySelector(int k) {
			this.key1 = k;
		}

		public K getKey(Edge<K, EV> edge) throws Exception {
			KeyMap.put(edge.getField(key1),edge.getField(key1+1));
			return edge.getField(key1);
		}

		public EV getValue (Object k) throws Exception {
			key2= (EV) KeyMap.get(k);
			KeyMap.clear();
			return key2;

		}
	}


	///////code for partitioner/////////
	private static class DegreeBased<K, EV, T> implements Partitioner<T> {
		private static final long serialVersionUID = 1L;
		SampleKeySelector<T, ?> keySelector;
		private final Table<Long, Long, Long> degree = HashBasedTable.create();   //for <partition.no, vertexId, Degree>
		private final HashMap<Long, List<Tuple2<Long, Long>>> result = new HashMap<>();
		private final List<Double> load = new ArrayList<>(); //for load of each partiton
		private final List<Long> subset = new ArrayList<>();
		private Long k;   //no. of partitions
		private Double loadlimit=0.0;
		private int m=0;  // no. of edges

		public DegreeBased(SampleKeySelector<T, ?> keySelector,int m) {
			this.keySelector = keySelector;
			this.k = (long) 4;
			this.m=m;
			this.loadlimit=(k*1.1+m)/k;;
		}

		@Override
		public int partition(Object key, int numPartitions) {

			Long target = 0L;
			try {
				target = (Long) keySelector.getValue(key);
			} catch (Exception e) {
				e.printStackTrace();
			}

			long source = (long) key;
			System.out.println("source");
			System.out.println(source);
			System.out.println("target");
			System.out.println(target);
			System.out.println("end");

			int h = 0;


			if (degree.isEmpty()) {
				for (int j = 0; j < k; j++) {
					load.add(j, 0.0);
				}
				load.set(0, 1.0);
				degree.put((long) 0, source, (long) 0);
				degree.put((long) 0, target, (long) 1);
				List<Tuple2<Long, Long>> L = new ArrayList<>();
				L.add(new Tuple2<>(source, target));
				result.put((long) 0, L);
				h = 0;

			} else {
				//condition 1 both vertices in same partition
				for (int i = 0; i < k; i++) {

					int value = 0;
					value = getValue(source, target, i);
					subset.add(i, (long) value);

					//get the value of S(k) from each partition using return
				}

				h = cost(source, target);

				Long d1 = degree.get((long) h, source);
				Long d2 = degree.get((long) h, target);
				if (d1 == null) {
					d1 = (long) 0;
				}
				if (d2 == null) {
					d2 = (long) 0;
				}
				//d1++;
				d2++;
				Double l = load.get(h);
				l++;
				load.set(h, l);
				degree.put((long) h, source, d1);
				degree.put((long) h, target, d2);
				if (result.get((long) h) != null) {
					List<Tuple2<Long, Long>> L = result.get((long) h);
					L.add(new Tuple2<>(source, target));
					result.put((long) h, L);
				} else {
					List<Tuple2<Long, Long>> L = new ArrayList<>();
					L.add(new Tuple2<>(source, target));
					result.put((long) h, L);
				}
				subset.clear();
			}

			return h;
		}

		public int cost(Long source, Long target) {
			Long max = subset.get(0);
			int sub = 0;
			for (int j = 1; j < k; j++) {

				if (max < subset.get(j) && load.get(j).compareTo(loadlimit)<0) {
					max = subset.get(j);
					sub =j;
				}

				else if (max.equals( subset.get(j)) && load.get(j).compareTo(loadlimit)<0 && subset.get(j).equals(1L)){
					if(degree.get((long)j,source)!=null && degree.get((long)sub,target)!=null ){
						if(degree.get((long)j,source)< degree.get((long)sub,target)){
							max = subset.get(j);
							sub =j;
						}
						else if(load.get(j).compareTo(load.get(sub))<0){
							max = subset.get(j);
							sub =j;
						}

					}
					else if(degree.get((long)j,target)!=null && degree.get((long)sub,source)!=null){
						if(degree.get((long)j,target)< degree.get((long)sub,source)){
							max = subset.get(j);
							sub =j;
						}
						else if(load.get(j).compareTo(load.get(sub))<0){
							max = subset.get(j);
							sub =j;
						}

					}
					else{
						if(load.get(j).compareTo(load.get(sub))<0)
						{
							max = subset.get(j);
							sub =j;
						}
					}

				}
				else if(max.equals( subset.get(j)) && subset.get(j)==0 && load.get(j).compareTo(load.get(sub))<0)
				{
					max = subset.get(j);
					sub =j;
				}
			}


			return sub;


		}

		public int getValue(Long source, Long target, int p) {
			{
				int i = 0;
				if (degree.contains((long) p, source) && degree.contains((long) p, target)) {
					i = 2;
				} else if (degree.contains((long) p, source) && !degree.contains((long) p,target)) {
					i = 1;
				} else if (!degree.contains((long) p, source) && degree.contains((long) p, target)) {
					i = 1;
				} else {
					i = 0;
				}
				return i;
			}

		}
	}



		private static DataStream<Edge<Long, NullValue>> getGraphStream(StreamExecutionEnvironment env) throws IOException {
			return env.fromCollection(getEdges());
		}

		public static final List<Edge<Long, NullValue>> getEdges() throws IOException {
			List<Edge<Long, NullValue>> edges = new ArrayList<>();

			FileReader inputFile = new FileReader("/Users/zainababbas/gelly3/gelly-streaming/inputj");
			//Instantiate the BufferedReader Class

			BufferedReader bufferReader = new BufferedReader(inputFile);

			String line;
			// Read file line by line and print on the console
			while ((line = bufferReader.readLine()) != null) {
				String[] fields = line.split("\\,");
				long src = Long.parseLong(fields[0]);
				long trg = Long.parseLong(fields[1]);
				//if(src!=trg){
				edges.add(new Edge<>(src, trg, NullValue.getInstance()));}
			//	}

			return edges;
		}
}

