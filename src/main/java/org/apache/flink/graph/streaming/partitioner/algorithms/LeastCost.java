package org.apache.flink.graph.streaming.partitioner.algorithms;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.streaming.partitioner.until.CustomPartitioners;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by zainababbas on 10/04/16.
 */
public class LeastCost {

	public static void main(String[] args) throws Exception {

		List<Tuple2<Long, Long>> edges = new ArrayList<>();
		getEdges(edges);
		//System.out.print(edges);
		Partition P = new Partition((long) 4);
		P.partition(edges);
		System.out.println("lala");
	}

	private static void getEdges(List<Tuple2<Long, Long>> E) {
		E.add(new Tuple2<Long, Long>(1L, 2L));
		E.add(new Tuple2<Long, Long>(1L, 3L));
		E.add(new Tuple2<Long, Long>(1L, 5L));
		E.add(new Tuple2<Long, Long>(2L, 3L));
		E.add(new Tuple2<Long, Long>(6L, 7L));
		E.add(new Tuple2<Long, Long>(8L, 9L));
	}

	private static class Partition extends CustomPartitioners {
		private final HashMap<Long,List<Long>> vertices = new HashMap<>();  //for <partition.no, vertexId>
		private final List<Long> load = new ArrayList<>(); //for load of each partiton
		private final List<Long> cost = new ArrayList<>();
		private Long k;   //no. of partitions

		public Partition(Long n) {
			k=n;
		}

		public void partition(List<Tuple2<Long,Long>> Edges)
		{


			for(int j=0; j<k;j++){
				load.add(j, (long) 0);
			}

			Edges.stream().forEach(s -> {
				Long V1 = s.f0;
				Long V2 = s.f1;

				if(vertices.isEmpty())
				{
					load.set(0, (long) 1);
					List<Long> L = new ArrayList<>();
					L.add(V1);
					L.add(V2);
					vertices.put((long) 0, L);

				}
				else{
					//condition 1 both vertices in same partition
					for(int i=0; i<k;i++) {

						int c=0;
						c = getValue(s, i);
						cost.add(i, (long) c);

						//get the value of S(k) from each partition using return
					}

					CompareCost(s);
					System.out.println("one");
					cost.clear();

				}
				System.out.println(s);
			});


		}
		public void CompareCost(Tuple2<Long,Long> E) {
			Long max = cost.get(0);
			int sub = 0;
			List<Integer> tie= new ArrayList<>();
			for (int j = 1; j < k; j++) {

				if (max < cost.get(j)) {
					max = cost.get(j);
					sub =j;
				}

				else if (max == cost.get(j)){
					if(!tie.contains(sub)){
						tie.add(sub);
					}
					if(!tie.contains(j)){
						tie.add(j);
					}

				}
			}

			addEdge(tie,sub,E);


		}


		public void addEdge(List<Integer> Tie, int Max, Tuple2<Long,Long> E){

			if(!Tie.isEmpty() && Max==0L){
				int max =  Tie.get(0);
				int s= Tie.size();
				int val_sub=0;
				int val = (int) (cost.get(max)+load.get(max));
				for(int  j =1; j<= (s-1); j++){

					if(val >= (int) (cost.get(j)+load.get(j)))
					{
						val=(int) (cost.get(j)+load.get(j));
						val_sub=j;
						System.out.println("Hola   1!!");
					}
				}

				Long V1 = E.f0;
				Long V2 = E.f1;

				Long l = load.get(val_sub);
				l++;
				load.set(val_sub,l);

				if(vertices.get((long) val_sub)!=null)
				{
					List<Long> L= vertices.get((long) val_sub);
					L.add(E.f0);
					L.add(E.f1);
					vertices.put((long) val_sub, L);
				}
				else {
					List<Long> L = new ArrayList<>();
					L.add(E.f0);
					L.add(E.f1);
					vertices.put((long) val_sub, L);
				}
			}
			else{
				System.out.println("Hola   2 !!");
				Long V1 = E.f0;
				Long V2 = E.f1;
				Long l = load.get(Max);
				l++;
				load.set(Max,l);
				if(vertices.get((long) Max)!=null)
				{
					List<Long> L= vertices.get((long) Max);
					L.add(E.f0);
					L.add(E.f1);
					vertices.put((long) Max, L);
				}
				else {
					List<Long> L = new ArrayList<>();
					L.add(E.f0);
					L.add(E.f1);
					vertices.put((long) Max, L);
				}

			}

		}

		public int getValue(Tuple2<Long,Long> E, int p) {
			int i = 0;
			Long V1 = E.f0;
			Long V2 = E.f1;
			List<Long> L = new ArrayList<>();
			L = vertices.get((long) p);
			if (L != null) {
				if (L.contains((long) V1) && L.contains((long) V2)) {

					System.out.println("one");
					i = 2;
				} else if (L.contains((long) V1) && !L.contains((long) V2)) {

					System.out.println("one");
					i = 1;
				} else if (!L.contains((long) V1) && L.contains((long) V2)) {
					System.out.println("one");
					i = 1;
				} else {
					i = 0;
				}

			}return i;
		}

	}
}