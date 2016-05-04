package org.apache.flink.graph.streaming.partitioner;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.GraphStream;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.graph.streaming.example.WindowTriangles;
import org.apache.flink.graph.streaming.example.util.Candidates;
import org.apache.flink.graph.streaming.example.util.DisjointSet;
import org.apache.flink.graph.streaming.library.BipartitenessCheck;
import org.apache.flink.graph.streaming.library.ConnectedComponents;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import scala.collection.parallel.ParIterableLike;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class ExampleTest {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Edge<Long, NullValue>> edges = getGraphStream(env);
		SingleOutputStreamOperator g = edges.flatMap(new FlatMapFunction<Edge<Long,NullValue>, Long>() {
			@Override
			public void flatMap(Edge<Long, NullValue> value, Collector<Long> out) throws Exception {
				 out.collect(value.getTarget());
				out.close();
			}

		});
g.print();
		//edges.partitionCustom(new test(new SampleKeySelector(0)),new SampleKeySelector(0)).print();
//		GraphStream<Long, NullValue, NullValue> graph = new SimpleEdgeStream<>(edges.partitionCustom(new test(new SampleKeySelector(0)),new SampleKeySelector(0)), env);
//		DataStream<DisjointSet<Long>> cc = graph.aggregate(new ConnectedComponents<Long, NullValue>(5000));
 //       cc.print();
//
//		DataStream<Edge<Long, NullValue>> edges2 = getGraphStream(env);
		//edges.partitionCustom(new test(new SampleKeySelector(0)),new SampleKeySelector(0)).print();
//		GraphStream<Long, NullValue, NullValue> graph2 = new SimpleEdgeStream<>(edges2, env);
//		DataStream<DisjointSet<Long>> cc2 = graph.aggregate(new ConnectedComponents<Long, NullValue>(5000));
//		cc2.print();
		/*cc.flatMap(new WindowTriangles.FlattenSet()).keyBy(0)
				.timeWindow(Time.of(2000, TimeUnit.MILLISECONDS))
				.fold(new Tuple2<Long, Long>(0l, 0l), new WindowTriangles.IdentityFold()).print();*/
		env.execute("testing custom partitioner");
		}

    private static class SampleKeySelector<K, EV> implements KeySelector<Edge<K, EV>, K> {
	private final int key;

	public SampleKeySelector(int k) {
		this.key = k;
	}

	public K getKey(Edge<K, EV> edge) throws Exception {
		//System.out.println((Long) edge.getField(key));
		return edge.getField(key);
	}
}
	private static class test<T> implements Partitioner<T> {
		private static final long serialVersionUID = 1L;

		//private int[] returnArray = new int[1];
		KeySelector<T, ?> keySelector;

		public test(KeySelector<T, ?> keySelector) {
			this.keySelector = keySelector;
		}

	/*	public int[] selectChannels(SerializationDelegate<StreamRecord<T>> record,
									int numberOfOutputChannels) {
			Object key;
			try {
				key =2;
				//key = keySelector.getKey(record.getInstance().getValue());
			} catch (Exception e) {
				throw new RuntimeException("Could not extract key from " + record.getInstance().getValue(), e);
			}
			returnArray[0] = (int) key;

			return returnArray;
		}


		@Override
		public String toString() {
			return "new";
		}*/

		@Override
		public int partition(Object key, int numPartitions) {
			System.out.println(numPartitions);
			long k = (long) key;
			int h = (int) k;

			return h%4;
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
		edges.add(new Edge<>(1L, 5L, NullValue.getInstance()));
		edges.add(new Edge<>(6L, 7L, NullValue.getInstance()));
		edges.add(new Edge<>(8L, 9L, NullValue.getInstance()));
		return edges;
	}
}

