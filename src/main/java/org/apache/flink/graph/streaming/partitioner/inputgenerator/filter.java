package org.apache.flink.graph.streaming.partitioner.inputgenerator;


import org.apache.flink.api.java.ExecutionEnvironment;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;

/**
 * Created by zainababbas on 14/02/2017.
 */
public class filter {


	private int vertices;
	private long edges;

	public filter(){

		edges = 0;
		vertices = 0;

	}
	public List<Edge> filterd(String Input) {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		long begin_time = System.currentTimeMillis();
		TreeSet<Integer> vertices_tree = new TreeSet<Integer>();
		TreeSet<Edge> edges_tree = new TreeSet<Edge>();
		HashMap<Integer, Integer> degree = new HashMap<Integer, Integer>();


		FileInputStream fis = null;
		try {
			fis = new FileInputStream(new File(Input));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		InputStreamReader isr = new InputStreamReader(fis);
		BufferedReader in = new BufferedReader(isr);
		String line;
		try {
			while ((line = in.readLine()) != null) {

				String values[] = line.split("\\,");
				int u = Integer.parseInt(values[0]);
				int v = Integer.parseInt(values[1]);

					Edge t = new Edge(u,v);
					if (edges_tree.add(t)) {
						edges++;
					}

					if (vertices_tree.add(u)) {
						vertices++;
					}
					if (vertices_tree.add(v)) {
						vertices++;
					}

			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		List<Edge> dataset ;
		return dataset = new ArrayList<Edge>(edges_tree);


	}

}
