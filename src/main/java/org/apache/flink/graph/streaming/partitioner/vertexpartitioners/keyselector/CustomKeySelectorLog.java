package org.apache.flink.graph.streaming.partitioner.vertexpartitioners.keyselector;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

/**
 * Created by zainababbas on 07/02/2017.
 */
public class CustomKeySelectorLog<K, EV> implements Serializable, KeySelector<Tuple3<K, List<EV>,EV>, K> {
	private final int key1;
	private static final HashMap<Object, Object> DoubleKey = new HashMap<>();

	public CustomKeySelectorLog(int k) {
		this.key1 = k;
	}

	public K getKey(Tuple3<K, List<EV>,EV> vertices) throws Exception {
		DoubleKey.put(vertices.getField(key1),vertices.getField(key1 + 1));
		return vertices.getField( key1);
	}

	public Object getValue(Object k) throws Exception {
		Object key2 = DoubleKey.get(k);
		DoubleKey.clear();
		return key2;
	}


}
