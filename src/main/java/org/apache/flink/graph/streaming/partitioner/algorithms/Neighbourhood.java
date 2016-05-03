package org.apache.flink.graph.streaming.partitioner.algorithms;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.streaming.partitioner.until.CustomPartitioners;
import org.apache.flink.hadoop.shaded.com.google.common.collect.HashBasedTable;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Basic neighbourhood based partitioner
 */
public class Neighbourhood {


    public static void main(String[] args) throws Exception {

        List<Tuple2<Long, Long>> edges = new ArrayList<>();
        getEdges(edges);
        //System.out.print(edges);
        Partition P = new Partition((long) 4);
        P.partition(edges);
        System.out.println("lala");

    }

    private static void getEdges(List<Tuple2<Long,Long>> E)
    {
        E.add(new Tuple2<Long, Long>(1L, 2L));
        E.add(new Tuple2<Long, Long>(3L, 4L));
        E.add(new Tuple2<Long, Long>(5L, 6L));
        E.add(new Tuple2<Long, Long>(7L, 8L));
        E.add(new Tuple2<Long, Long>(4L, 7L));
        E.add(new Tuple2<Long, Long>(4L, 8L));
    }

    private static class Partition extends CustomPartitioners {

        private final Table<Long,Long,Long> Degree =  HashBasedTable.create();   //for <partition.no, vertexId, Degree>
        private final HashMap<Long,List<Tuple2<Long,Long>>> Result = new HashMap<>();
        private final List<Long> load = new ArrayList<>(); //for load of each partiton
        private final List<Long> subset = new ArrayList<>();
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

               if(Degree.isEmpty())
               {
                   load.set(0, (long) 1);
                   Degree.put((long) 0, V1,(long) 1);
                   Degree.put((long) 0, V2,(long) 1);
                   List<Tuple2<Long,Long>> L = new ArrayList<>();
                   L.add(s);
                   Result.put((long) 0, L);
               }
               else{
               //condition 1 both vertices in same partition
                for(int i=0; i<k;i++) {

                    int value=0;
                    value = getValue(s, i);
                    subset.add(i, (long) value);

                    //get the value of S(k) from each partition using return
                }

                   Cost(s);
                        System.out.println("one");
                   subset.clear();

                }
           System.out.println(s);
           });


       }

        public void Cost(Tuple2<Long,Long> E) {
            Long max = subset.get(0);
            int sub = 0;
             List<Integer> tie= new ArrayList<>();
            for (int j = 1; j < k; j++) {

                if (max < subset.get(j)) {
                    max = subset.get(j);
                    sub =j;
                }

                else if (max == subset.get(j)){
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

            if(!Tie.isEmpty()){
                int max =  Tie.get(0);
                int s= Tie.size();
                int val_sub=0;
                int val = (int) (subset.get(max)+load.get(max));
               for(int  j =1; j<= (s-1); j++){

                 if(val >= (int) (subset.get(j)+load.get(j)))
                 {
                     val=(int) (subset.get(j)+load.get(j));
                     val_sub=j;
                     System.out.println("Hola   1!!");
                 }
               }

                Long V1 = E.f0;
                Long V2 = E.f1;
                Long d1=Degree.get((long) val_sub, V1);
                Long d2=Degree.get((long) val_sub, V2);
                if(d1==null)
                {d1=(long) 0;}
                if(d2==null)
                {d2=(long) 0;}       //return 2 as 2 vertices here
                d1++;
                d2++;
                Long l = load.get(val_sub);
                l++;
                load.set(val_sub,l);
                Degree.put((long) val_sub, V1,d1);
                Degree.put((long) val_sub, V2,d2);
                if(Result.get((long) val_sub)!=null)
                {
                    List<Tuple2<Long,Long>> L= Result.get((long) val_sub);
                    L.add(E);
                    Result.put((long) val_sub, L);
                }
                else {
                    List<Tuple2<Long, Long>> L = new ArrayList<>();
                    L.add(E);
                    Result.put((long) val_sub, L);
                }
            }
            else{
                System.out.println("Hola   2 !!");
                Long V1 = E.f0;
                Long V2 = E.f1;
                Long d1=Degree.get((long) Max, V1);
                Long d2=Degree.get((long) Max, V2);
                if(d1==null)
                {d1=(long) 0;}
                if(d2==null)
                {d2=(long) 0;}       //return 2 as 2 vertices here
                d1++;
                d2++;
                Long l = load.get(Max);
                l++;
                load.set(Max,l);
                Degree.put((long) Max, V1,d1);
                Degree.put((long) Max, V2,d2);
                if(Result.get((long) Max)!=null)
                {
                    List<Tuple2<Long,Long>> L= Result.get((long) Max);
                    L.add(E);
                    Result.put((long) Max, L);
                }
                else {
                    List<Tuple2<Long, Long>> L = new ArrayList<>();
                    L.add(E);
                    Result.put((long) Max, L);
                }

            }

        }

        public int getValue(Tuple2<Long,Long> E, int p)
        {
            int i =0;
            Long V1 = E.f0;
            Long V2 = E.f1;
            if (Degree.contains((long) p, (long) V1) && Degree.contains((long) p, (long) V2)) {

                System.out.println("one");
                i =2;
            }
            else if(Degree.contains((long) p, (long) V1) && !Degree.contains((long) p, (long) V2)){

                System.out.println("one");
                i=1;
            }

            else if(!Degree.contains((long) p, (long) V1) && Degree.contains((long) p, (long) V2)){
                System.out.println("one");
                i=1;
            }
            else {
                i=0;
            }
           return i;
        }

    }



}
