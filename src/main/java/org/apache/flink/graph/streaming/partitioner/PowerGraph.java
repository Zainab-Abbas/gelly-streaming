package org.apache.flink.graph.streaming.partitioner;

import org.apache.flink.graph.streaming.partitioner.until.Partitioner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by zainababbas on 12/04/16.
 */
public class PowerGraph {

	public static void main(String[] args) throws Exception {

		List<InputData> edges = new ArrayList<>();
		getEdges(edges);
		//System.out.print(edges);
		Partition P = new Partition((long) 4);
		P.partition(edges);
		System.out.println("lala");
	}
	private static void getEdges(List<InputData> E) {

		E.add(0,new InputData(1L,4L,1L,2L));
		E.add(0,new InputData(7L,3L,1L,1L));
		E.add(0,new InputData(2L,4L,1L,2L));
		E.add(0,new InputData(6L,5L,1L,1L));

	}

	private static class InputData {

		private long Vertex1;
		private long Vertex2;
		private long Degree1;
		private long Degree2;

		public InputData(Long v1, Long v2, Long d1, Long d2)
		{
			Vertex1=v1;
			Vertex2=v2;
			Degree1=d1;
			Degree2=d2;
		}


	}
	private static class Partition extends Partitioner {
		private final HashMap<Long,List<Long>> vertices = new HashMap<>();  //for <partition.no, vertexId>
		private final List<Long> load = new ArrayList<>(); //for load of each partiton
		private final List<Long> cost = new ArrayList<>();
		private final List<Long> breaker = new ArrayList<>();
		private Long k;   //no. of partitions

		public Partition(Long n) {
			k=n;
		}

		public void partition(List<InputData> Edges)
		{


			for(int j=0; j<k;j++){
				load.add(j, (long) 0);
			}

				for(int j=0; j<k;j++){
					breaker.add(j, (long) 0);
				}

			Edges.stream().forEach(s -> {

				Long V1 = s.Vertex1;
				Long V2 = s.Vertex2;

				if(vertices.isEmpty())
				{
					load.set(0, (long) 1);
					List<Long> L = new ArrayList<>();
					L.add(V1);
					L.add(V2);
					vertices.put((long) 0,L);
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
							for(int j=0; j<k;j++){
								breaker.set(j, (long) 0);
				}
				}
				System.out.println(s);
			});


		}
		public void CompareCost(InputData E) {
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


		public void addEdge(List<Integer> Tie, int Max, InputData E){

			if(!Tie.isEmpty() && Max==0L){
				int max =  Tie.get(0);
				int s= Tie.size();
				int val_sub=0;
				Long val=0L;
				if(breaker.get(0)!=0L){
				 val = breaker.get(max);
					for(int  j =1; j<= (s-1); j++){

						if(val <= breaker.get(j))
						{
							val=breaker.get(j);
							val_sub=j;
							System.out.println("Hola   1!!");
						}
					}}
				else
				{
					val = load.get(max);
					for(int  j =1; j<= (s-1); j++){

						if(val > load.get(j))
						{
							val=load.get(j);
							val_sub=j;
							System.out.println("Hola   2!!");
						}
					}
				}

				Long V1 = E.Vertex1;
				Long V2 = E.Vertex2;
				Long l = load.get(val_sub);
				l++;
				load.set(val_sub,l);
				if(vertices.get((long) val_sub)!=null)
				{
					List<Long> L= vertices.get((long) val_sub);
					L.add(V1);
					L.add(V2);
					vertices.put((long) val_sub, L);
				}
				else {
					List<Long> L = new ArrayList<>();
					L.add(V1);
					L.add(V2);
					vertices.put((long) val_sub, L);
				}
			}
			else{
				System.out.println("Hola   2 !!");
				Long V1 = E.Vertex1;
				Long V2 = E.Vertex2;
				Long l = load.get(Max);
				l++;
				load.set(Max,l);
				if(vertices.get((long) Max)!=null)
				{
					List<Long> L= vertices.get((long) Max);
					L.add(V1);
					L.add(V2);
					vertices.put((long) Max, L);
				}
				else {
					List<Long> L = new ArrayList<>();
					L.add(V1);
					L.add(V2);
					vertices.put((long) Max, L);
				}

			}

		}

		public int getValue(InputData E, int p) {
			int i = 0;
			Long V1 = E.Vertex1;
			Long V2 = E.Vertex2;
			List<Long> L = new ArrayList<>();
			L = vertices.get((long) p);
			if (L != null) {
				if (L.contains(V1)&& L.contains(V2)){

					System.out.println("one");
					i = 2;
				}
				else if (L.contains(V1) && !L.contains(V2)) {

					System.out.println("one");
					i = 1;
					breaker.set(p, E.Degree1);
				} else if (!L.contains(V1) && L.contains(V2)) {
					System.out.println("one");
					i = 1;
					breaker.set(p, E.Degree2);
				} else {
					i = 0;
				}

			}return i;
		}
	}



}
