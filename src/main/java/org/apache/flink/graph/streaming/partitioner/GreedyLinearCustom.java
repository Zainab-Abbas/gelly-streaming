package org.apache.flink.graph.streaming.partitioner;

/**
 * Created by zainababbas on 27/04/16.
 */

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;

import java.util.*;


import java.util.ArrayList;
import java.util.List;


public class GreedyLinearCustom{

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Tuple2<Long, List<Long>>> vertices = getGraphStream(env);
		vertices.getTransformation().getOutputType();
		vertices.partitionCustom(new test(new SampleKeySelector(0),5),new SampleKeySelector(0)).print();

		env.execute("testing custom partitioner");
		System.out.println("lala");
	}

	private static class SampleKeySelector<K, EV> implements KeySelector<Tuple2<K, List<EV>>, K> {
		private final int key;
		private static final HashMap<Long,List<Long>> DoubleKey = new HashMap<>();

		public SampleKeySelector(int k) {
			this.key = k;
		}

		public K getKey(Tuple2<K, List<EV>> vertices) throws Exception {
			DoubleKey.put(vertices.getField(key),vertices.getField(key+1));
			return vertices.getField(key);
		}

		public List<Long> getValue (Object k) throws Exception {
			return DoubleKey.get((long) k);
		}
	}


	///////code for partitioner/////////

	private static class test<K, EV, T> implements Partitioner<T> {
		private static final long serialVersionUID = 1L;
		SampleKeySelector<T, ?> keySelector;
		private final HashMap<Long,List<Long>> Result = new HashMap<>();//partitionid, list of vertices placed
		private final List<Long> load = new ArrayList<>(); //for load of each partiton
		private Long k;  //no. of partitions
		private Double C;     // no. of vertices

		private final List<Tuple2<Long,Long>> edges=new ArrayList<>();

		public test(SampleKeySelector<T, ?> keySelector, int m)
		{
			this.keySelector = keySelector;
			this.k=(long) 4;
			this.C= (double)m/(double)k;

		}

		@Override
		public int partition(Object key, int numPartitions) {

			List<Long> neighbours=new ArrayList<>();
			try {
				neighbours=keySelector.getValue(key);
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

			if(Result.isEmpty())
			{
				for(int j=0; j<k;j++){
					load.add(j, (long) 0);
				}
				load.set(0, (long) 1);
				load.set(0, (long) 1);
				List<Long> L = new ArrayList<>();
				L.add(source);
				Result.put((long) 0, L);
				h=0;

			}



			else {
				List<Double> num = new ArrayList<>();
				int n=0;
				for(int j=0; j<k;j++){
					num.add(j, 0.0);
				}
				for (int i = 0; i < k; i++) {
					n=getValue(i,neighbours);
					num.set(i, ((double)n / (1 - ((double)load.get(i)/C))));
				}

				Double I=0.0;
				Long l=0L;
				int index=0;
				I=num.get(0);
				for (int i = 1; i < k; i++) {
					if(I<num.get(i))
					{
						I=num.get(i);
						index=i;
					}
					if(I.compareTo(num.get(i)) ==0)
					{
						if(load.get(i) < load.get(index))
						{
							I=num.get(i);
							index=i;
						}
					}

				}

				h=index;
				l=load.get(index);
				l=l+1;
				load.set(index, l);
				if(Result.get((long)index) ==null)
				{
					List<Long> L = new ArrayList<>();
					L.add(source);
					Result.put((long) index, L);
				}
				else{
					List<Long> L = new ArrayList<>();
					L=Result.get((long) index);
					L.add(source);
					Result.put((long) index, L);
				}

			}

			return h;
		}

		public int getValue(int p,List<Long> n){

			int ne=0;
			List<Long> list = new ArrayList<>();
			for(int i=0;i<n.size();i++)
			{
				Long v=n.get(i);
				list =Result.get((long) p);
				if(list!=null) {
					if (list.contains(v)) {
						ne++;
					}
				}
			}

			return ne;
		}


	}

	private static DataStream<Tuple2<Long, List<Long>>> getGraphStream(StreamExecutionEnvironment env) {
		return env.fromCollection(getVertices());
	}

	public static final List<Tuple2<Long, List<Long>>> getVertices() {
		List<Tuple2<Long, List<Long>>> vertices= new ArrayList<>();
		List<Long> n1 = new ArrayList<>();
		n1.add(4L);
		vertices.add(new Tuple2<Long, List<Long>>(1L, n1));
		List<Long> n2 = new ArrayList<>();
		n2.add(0,3L);
		n2.add(1,4L);
		vertices.add(new Tuple2<Long, List<Long>>(2L, n2));
		List<Long> n3 = new ArrayList<>();
		n3.add(0,2L);
		vertices.add(new Tuple2<Long, List<Long>>(3L, n3));
		List<Long> n4 = new ArrayList<>();
		n4.add(0,5L);
		vertices.add(new Tuple2<Long, List<Long>>(4L, n4));
		List<Long> n5 = new ArrayList<>();
		n5.add(0,4L);
		vertices.add(new Tuple2<Long, List<Long>>(5L, n5));
		return vertices;
	}


}

