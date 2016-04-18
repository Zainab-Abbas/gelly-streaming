package org.apache.flink.graph.streaming.partitioner.until;

/**
 * Created by zainababbas on 06/04/16.
 */
public abstract class Partitioner {

	// number of partitions
	private Long k;

	// partition method
	public void partition()
	{}

}
