package org.apache.flink.graph.streaming.partitioner.edgepartitioners.keyselector;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.graph.Edge;

import java.util.Hashtable;


/**
 * Created by zainababbas on 07/02/2017.
 */

public class CustomKeySelector2<K, EV> implements KeySelector<Edge<K, EV>, K> {
	private final int key1;
	private static Hashtable<Object,Object> keyMap = new Hashtable<>();

	public CustomKeySelector2(int k) {
		this.key1 = k;

	}
	public K getKey(Edge<K, EV> edge) throws Exception {
		keyMap.put(edge.getField(key1),edge.getField(key1+1));

		return edge.getField(key1);

	}

	public Object getValue (Object k) throws Exception {

		Object key2 = keyMap.get(k);
		return key2;

	}

}


