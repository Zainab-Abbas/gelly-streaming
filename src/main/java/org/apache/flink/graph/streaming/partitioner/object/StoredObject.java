package org.apache.flink.graph.streaming.partitioner.object;

import java.io.Serializable;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * Created by zainababbas on 05/02/2017.
 */
public class StoredObject implements Serializable {

	private TreeSet<Byte> partitions;
	private int degree;

	public StoredObject() {
		partitions = new TreeSet<Byte>();
		degree = 0;
	}


	public Iterator<Byte> getPartitions(){
		return partitions.iterator();
	}

	
	public void addPartition(int m){
		if (m==-1){ System.out.println("ERRORE! record.addPartition(-1)"); System.exit(-1);}
		partitions.add( (byte) m);
	}

	public void addAll(TreeSet<Byte> tree){
		partitions.addAll(tree);
	}


	public boolean hasReplicaInPartition(int m){
		return partitions.contains((byte) m);
	}

	
	public int getReplicas(){
		return partitions.size();
	}
	
	public int getDegree() {
		return degree;
	}
	
	public void incrementDegree() {
		this.degree++;
	}

	public static TreeSet<Byte> intersection(StoredObject x, StoredObject y){
		TreeSet<Byte> result = (TreeSet<Byte>) x.partitions.clone();
		result.retainAll(y.partitions);
		return result;
	}

}
