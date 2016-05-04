package org.apache.flink.graph.streaming.partitioner.algorithms;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.streaming.partitioner.algorithms.until.CustomPartitioners;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by zainababbas on 06/04/16.
 */
public class Fennel {
	public static void main(String[] args) throws Exception {

		List<Tuple2<Long, List<Long>>> vertices = new ArrayList<>();
		getVertices(vertices);
		//System.out.print(edges);
		Partition P = new Partition((long) 4);
		//P.partition(vertices);
		P.values(vertices);
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
		n2.add(0,6L);
		V.add(new Tuple2<Long, List<Long>>(2L, n2));
		List<Long> n3 = new ArrayList<>();
		n3.add(0,2L);
		V.add(new Tuple2<Long, List<Long>>(3L, n3));
		List<Long> n4 = new ArrayList<>();
		n4.add(0,1L);
		n4.add(1,5L);
		V.add(new Tuple2<Long, List<Long>>(4L, n4));
		List<Long> n5 = new ArrayList<>();
		n5.add(0,4L);
		V.add(new Tuple2<Long, List<Long>>(5L, n5));
		List<Long> n6 = new ArrayList<>();
		n6.add(0,2L);
		V.add(new Tuple2<Long, List<Long>>(6L, n6));
	}

	private static class Partition extends CustomPartitioners {

		private final HashMap<Long,List<Long>> Result = new HashMap<>();//partitionid, list of vertices placed
		private final List<Double> load = new ArrayList<>(); //for load of each partiton
		private Long k;  //no. of partitions

		private final List<Tuple2<Long,Long>> edges=new ArrayList<>();
		private double alpha=0;  //parameters for formula
		private double gamma=1.5;
		private double loadlimit= 0.0;     //k*v+n/n

		public Partition(Long n) {
		k=n;
		}

		public void values(List<Tuple2<Long,List<Long>>> Edges){
			int s=0;
			s=Edges.size();
			//List<Tuple2<Long,Long>> edges=new ArrayList<>();
			Tuple2<Long,List<Long>> d=new Tuple2<>();
			Long t1=0L;
			Long t2=0L;
			List<Long> t3 =new ArrayList<>();
			for (int j=0; j<s;j++){
			d=Edges.get(j);
			t1=d.f0;t3=d.f1;
			for(int i=0;i<t3.size();i++)
			{
            t2=t3.get(i);
				if (t1 < t2)
				{
					if (!edges.contains(new Tuple2<Long,Long>(t1,t2)))
					{
					edges.add(new Tuple2<Long,Long>(t1,t2));
					}

				}
				else{
					if (!edges.contains(new Tuple2<Long,Long>(t2,t1)))
					{
						edges.add(new Tuple2<Long,Long>(t2,t1));
					}
				}
			}
		}
			Long n= Long.valueOf(Edges.size());
			Long m= Long.valueOf(edges.size());
			//alpha= ((Math.pow(k,0.5))*m)/Math.pow(n,1.5);
			alpha= (((Math.pow(k,0.5))*Math.pow(n,1.5))+m)/Math.pow(n,1.5);
			loadlimit=(k*1.1+n)/k;
		}

		public void partition(List<Tuple2<Long,List<Long>>> Edges) {

			for(int j=0; j<k;j++){
				load.add(j, 0.0);
			}

			Edges.stream().forEach(s -> {
				Long V1 = s.f0;
				List<Long> neighbours = s.f1;

				if(Result.isEmpty())
				{
					load.set(0, 1.1);
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
						num.set(i, (double) ((double)n-alpha*gamma*Math.pow(load.get(i),gamma-1)));

					}

					Double first=0.0;
					Double l=0.0;
					int index1=0;
					first=num.get(0);
					for (int i = 1; i < k; i++) {
                      if(first.compareTo(num.get(i))<0 && load.get(i).compareTo(loadlimit)<0)
					  {

						  first=num.get(i);
						  index1=i;

					  }
					}

					l=load.get(index1);
						l = l + 1;
						load.set(index1, l);
						if (Result.get((long) index1) == null) {
							List<Long> L = new ArrayList<>();
							L.add(V1);
							Result.put((long) index1, L);
						} else {
							List<Long> L = new ArrayList<>();
							L = Result.get((long) index1);
							L.add(V1);
							Result.put((long) index1, L);
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