package org.apache.flink.graph.streaming.partitioner.algorithms;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.streaming.partitioner.until.CustomPartitioners;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by zainababbas on 06/04/16.
 */
public class GreedyLinear {
	public static void main(String[] args) throws Exception {

		List<Tuple2<Long, List<Long>>> vertices = new ArrayList<>();
		getVertices(vertices);
		//System.out.print(edges);
		Partition P = new Partition((long) 4);
		//P.partition(vertices);
		P.partition(vertices);

		System.out.println("lala");

	}

	private static void getVertices(List<Tuple2<Long,List<Long>>> V)
	{
		List<Long> n1 = new ArrayList<>();
		n1.add(4L);
		V.add(new Tuple2<Long, List<Long>>(1L, n1));
		List<Long> n2 = new ArrayList<>();
		n2.add(0,3L);
		n2.add(1,4L);
		V.add(new Tuple2<Long, List<Long>>(2L, n2));
		List<Long> n3 = new ArrayList<>();
		n3.add(0,2L);
		V.add(new Tuple2<Long, List<Long>>(3L, n3));
		List<Long> n4 = new ArrayList<>();
		n4.add(0,5L);
		V.add(new Tuple2<Long, List<Long>>(4L, n4));
		List<Long> n5 = new ArrayList<>();
		n5.add(0,4L);
		V.add(new Tuple2<Long, List<Long>>(5L, n5));
	}

	private static class Partition extends CustomPartitioners {

		private final HashMap<Long,List<Long>> Result = new HashMap<>();//partitionId, list of vertices placed
		private final List<Long> load = new ArrayList<>(); //for load of each partition
		private Long k;  //no. of partitions

		private final List<Tuple2<Long,Long>> edges=new ArrayList<>();


		public Partition(Long n) {
			k=n;
		}


		public void partition(List<Tuple2<Long,List<Long>>> Edges) {

			for(int j=0; j<k;j++){
				load.add(j, (long) 0);
			}

			Edges.stream().forEach(s -> {
				Long V1 = s.f0;
				List<Long> neighbours = s.f1;

				if(Result.isEmpty())
				{
					load.set(0, (long) 1);
					List<Long> L = new ArrayList<>();
					L.add(V1);
					Result.put((long) 0, L);
				}
				else {
					List<Double> num = new ArrayList<>();
					int n=0;
					for(int j=0; j<k;j++){
						num.add(j, 0.0);
					}
					for (int i = 0; i < k; i++) {
						n=getValue(i,neighbours);
						num.set(i, (double) (n/(1-(load.get(i)/1.25))));   //1.25 is C = total vertices/no.of partitions

					}

					Long l=0L;
					int index=0;
					Double I=num.get(0);


					for (int i = 1; i < k; i++) {
						if(I<num.get(i))
						{
							I=num.get(i);
							index=i;
						}
						Double J=num.get(i);

						if(I.compareTo(J) == 0)
						{
							if(load.get(i) < load.get(index))
							{
								I=num.get(i);
								index=i;
							}
							else{
								System.out.println("yolo");
							}
						}

					}

					l=load.get(index);
					l=l+1;
					load.set(index, l);
					if(Result.get((long)index) ==null)
					{
						List<Long> L = new ArrayList<>();
						L.add(V1);
						Result.put((long) index, L);
					}
					else{
						List<Long> L = new ArrayList<>();
						L=Result.get((long) index);
						L.add(V1);
						Result.put((long) index, L);
					}

				}
			});

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

}
