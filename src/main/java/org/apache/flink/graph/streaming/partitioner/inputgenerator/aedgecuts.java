package org.apache.flink.graph.streaming.partitioner.inputgenerator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Created by zainababbas on 21/02/2017.
 */
public class aedgecuts {
	public static void main(String[] args) throws Exception {
		//take list of edges, check for each end vertex if both are in the partition or not, if not mark a 1 in the hashlist, otherwise 0.
		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple4<Long, Long, Long, Long>> fresult = null;
		DataSet<String> text = env.readTextFile(InputPath);

		DataSet<Tuple2<Long,Long>> edges = text.map(new MapFunction<String, Tuple2<Long,Long>>() {
			@Override
			public Tuple2<Long,Long> map(String value) {
				String[] fields = value.split("\\,");
				Long src = Long.parseLong(fields[0]);
				Long trg = Long.parseLong(fields[1]);
				return new Tuple2<>(src, trg);
			}
		});

		long s=0;
for (int i=1;i<=k;i++) {
	for (int j = 1; j <= k; j++) {
Long ii= new Long(i);
		Long jj= new Long(j);
		if(i!=j) {
			DataSet<String> p1 = env.readTextFile(outputPath+"/"+i);

			DataSet<Tuple2<Long, Long>> p11 = p1.map(new MapFunction<String, Tuple2<Long, Long>>() {
				@Override
				public Tuple2<Long, Long> map(String value) {
					String[] fields = value.split("\\,");
					Long src = Long.parseLong(fields[0]);
				//	Long trg = Long.valueOf(finalI);
					Tuple2<Long,Long> T = new Tuple2<Long, Long>(src,ii);
					return T;
				}
			});
			System.out.print("oriingtuingsd");
			p1.print();
			DataSet<String> p2 = env.readTextFile(outputPath+"/"+j);

			DataSet<Tuple2<Long, Long>> p22 = p2.map(new MapFunction<String, Tuple2<Long, Long>>() {
				@Override
				public Tuple2<Long, Long> map(String value) {
					String[] fields = value.split("\\,");
					Long src = Long.parseLong(fields[0]);
				//	Long trg = Long.valueOf(finalJ);
					return new Tuple2<>(src,jj);
				}
			});


			DataSet<Tuple3<Long, Long, Long>> result = edges.join(p11).where(0).equalTo(0).with(new JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple3<Long, Long, Long>>() {
				@Override
				public Tuple3<Long, Long, Long> join(Tuple2<Long, Long> longLongTuple2, Tuple2<Long, Long> longLongTuple22) throws Exception {
					return new Tuple3<>(longLongTuple2.f0, longLongTuple2.f1, longLongTuple22.f1);
				}
			});

			result.print();
			DataSet<Tuple4<Long, Long, Long, Long>> result1 = result.join(p22).where(1).equalTo(0).with(new JoinFunction<Tuple3<Long, Long, Long>, Tuple2<Long, Long>, Tuple4<Long, Long, Long, Long>>() {
				@Override
				public Tuple4<Long, Long, Long, Long> join(Tuple3<Long, Long, Long> longLongLongTuple3, Tuple2<Long, Long> longLongTuple2) throws Exception {
					return new Tuple4<Long, Long, Long, Long>(longLongLongTuple3.f0, longLongLongTuple3.f1, longLongLongTuple3.f2, longLongTuple2.f1);
				}
			});

//result1.print();

			fresult = result1.filter(new FilterFunction<Tuple4<Long, Long, Long, Long>>() {
				@Override
				public boolean filter(Tuple4<Long, Long, Long, Long> longLongLongLongTuple4) throws Exception {
					if (longLongLongLongTuple4.f2 != longLongLongLongTuple4.f3)

						return true;
					else
						return false;
				}
			});


			s = s + fresult.count();

		}}}


		System.out.print("oufjnkfnksjnfksjksnsnssjdsnkdjdnnjksndksee"+s);

	}

	private static String InputPath = null;
	private static String outputPath = null;
	private static int k = 0;
	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			if (args.length != 3) {
				System.err.println("Usage: Dbh <input edges path> <output path> <log> <partitions> ");
				return false;
			}

			InputPath = args[0];
			outputPath = args[1];
			k = (int) Long.parseLong(args[2]);
		} else {
			System.out.println("Executing example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println(" Usage: Dbh <input edges path> <output path> <log> <partitions>");
		}
		return true;
	}


}
