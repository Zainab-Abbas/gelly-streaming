package org.apache.flink.graph.streaming.partitioner.inputgenerator;



import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.generator.CompleteGraph;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by zainababbas on 14/02/2017.
 */
public class filterVertices {



		public static void main(String[] args) throws Exception {

			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

			long vertexCount = 4;

			Graph<LongValue, NullValue, NullValue> graph = new CompleteGraph(env, vertexCount)
																   .setParallelism(1)
																   .generate();



			graph.filterOnEdges(new FilterFunction<Edge<LongValue, NullValue>>() {
				@Override
				public boolean filter(org.apache.flink.graph.Edge<LongValue, NullValue> edge) throws Exception {

					LongValue s= edge.getSource();
					LongValue t= edge.getTarget();
					if(s.compareTo(t)>0){
						return false;
					}
					return true;
				}
			}).getEdgeIds().writeAsCsv("/Users/zainababbas/working/gelly-streaming/2e", FileSystem.WriteMode.OVERWRITE);

			FileReader inputFile = new FileReader("/Users/zainababbas/working/gelly-streaming/10p");
			//Instantiate the BufferedReader Class

			BufferedReader bufferReader = new BufferedReader(inputFile);


			File file = new File("/Users/zainababbas/working/gelly-streaming/ver");

			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}

			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);


			System.out.println("Done");

			//Variable to hold the one line data
			String line;
			HashMap<Long, List<Long>> T = new HashMap<>();
			// Read file line by line and print on the console
			while ((line = bufferReader.readLine()) != null) {
				String[] fields = line.split("\\,");
				long src = Long.parseLong(fields[0]);
				long trg = Long.parseLong(fields[1]);

				ArrayList<Long> l = new ArrayList<>();
				if (T.get(src) != null) {
					l = (ArrayList<Long>) T.get(src);
				}


				l.add(trg);

				T.put(src, l);

				ArrayList<Long> n = new ArrayList<>();
				if (T.get(trg) != null) {
					n = (ArrayList<Long>) T.get(trg);
				}
				n.add(src);
				T.put(trg, n);
			}
			//Close the buffer reader
			bufferReader.close();
			for (Long key : T.keySet()) {
				bw.write(key + ":" + T.get(key));
				bw.write("\n");
			}
			bw.close();

			env.execute("Streaming Connected Components");
		}




}
