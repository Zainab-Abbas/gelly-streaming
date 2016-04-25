package org.apache.flink.graph.streaming.partitioner;

/**
 * Created by zainababbas on 25/04/16.
 */
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.graph.Edge;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.NullValue;

import java.util.ArrayList;
import java.util.List;


public class testing {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Edge<Long, NullValue>> edges = getGraphStream(env);
		edges.partitionCustom(new test(new NeighborKeySelector(0)),new NeighborKeySelector(0)).print();
		//edges.print();
		//edges.writeAsCsv("/Users/zainababbas/gelly/gelly-streaming/oppppp");
		env.execute("testing check");
		}

    private static class NeighborKeySelector<K, EV> implements KeySelector<Edge<K, EV>, K> {
	private final int key;

	public NeighborKeySelector(int k) {
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

