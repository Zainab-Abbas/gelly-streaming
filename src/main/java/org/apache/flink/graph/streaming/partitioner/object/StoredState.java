package org.apache.flink.graph.streaming.partitioner.object;

import org.apache.flink.graph.Edge;

import java.io.Serializable;
import java.util.HashMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zainababbas on 06/02/2017.
 */
public class StoredState implements Serializable{

		private HashMap<Long,StoredObject> record_map;
		private AtomicInteger[] machines_load_edges;
		private AtomicInteger[] machines_load_vertices;

		int MAX_LOAD;

		public StoredState(int k) {

			record_map = new HashMap<Long,StoredObject>();
			machines_load_edges = new AtomicInteger[k];
			for (int i = 0; i<machines_load_edges.length;i++){
				machines_load_edges[i] = new AtomicInteger(0);
			}
			machines_load_vertices = new AtomicInteger[k];
			for (int i = 0; i<machines_load_vertices.length;i++){
				machines_load_vertices[i] = new AtomicInteger(0);
			}
			MAX_LOAD = 0;
		//	if (GLOBALS.OUTPUT_FILE_NAME!=null){
		//		out = new DatWriter(GLOBALS.OUTPUT_FILE_NAME+".edges");
		//	}

			System.out.print("created");
		}

		public void incrementMachineLoadVertices(int m) {
			machines_load_vertices[m].incrementAndGet();
		}

		public int[] getMachines_loadVertices() {
			int [] result = new int[machines_load_vertices.length];
			for (int i = 0; i<machines_load_vertices.length;i++){
				result[i] = machines_load_vertices[i].get();
			}
			return result;
		}

		public StoredObject getRecord(Long x){
			if (!record_map.containsKey(x)){
				record_map.put(x, new StoredObject());
			}
			return record_map.get(x);
		}

		public int getNumVertices(){
			return record_map.size();
		}


		public int getTotalReplicas(){
			int result = 0;
			for (long x : record_map.keySet()){
				int r = record_map.get(x).getReplicas();
				if (r>0){
					result += record_map.get(x).getReplicas();
				}
				else{
					result++;
				}
			}
			return result;
		}


		public synchronized int getMachineLoad(int m) {
			return machines_load_edges[m].get();
		}

		public synchronized int getMachineVerticesLoad(int m) {
		return machines_load_vertices[m].get();
		}

		public synchronized void incrementMachineLoad(int m, Edge e) {
			int new_value = machines_load_edges[m].incrementAndGet();
			if (new_value>MAX_LOAD){
				MAX_LOAD = new_value;
			}
			//if (GLOBALS.OUTPUT_FILE_NAME!=null){
			//	out.write(e+": "+m+"\n");
		//	}
		}


		public int[] getMachines_load() {
			int [] result = new int[machines_load_edges.length];
			for (int i = 0; i<machines_load_edges.length;i++){
				result[i] = machines_load_edges[i].get();
			}
			return result;
		}


		public synchronized int getMinLoad() {
			int MIN_LOAD = Integer.MAX_VALUE;
			for (AtomicInteger load : machines_load_edges) {
				int loadi = load.get();
				if (loadi<MIN_LOAD){
					MIN_LOAD = loadi;
				}
			}
			return MIN_LOAD;
		}


		public int getMaxLoad() {
			return MAX_LOAD;
		}


		public SortedSet<Long> getVertexIds() {
			//if (GLOBALS.OUTPUT_FILE_NAME!=null){ out.close(); }
			return new TreeSet<Long>(record_map.keySet());
		}

	}

