package org.apache.flink.graph.streaming.partitioner.inputgenerator;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by zainababbas on 21/02/2017.
 */
public class edgecuts {
	public static void main(String[] args) throws Exception {
		//take list of edges, check for each end vertex if both are in the partition or not, if not mark a 1 in the hashlist, otherwise 0.



	HashMap<Tuple2<Long, Long>, Long> T = new HashMap<>();

	FileReader inputFile = new FileReader("/Users/zainababbas/working/gelly-streaming/edges/smalle");

	//Instantiate the BufferedReader Class

	BufferedReader bufferReader = new BufferedReader(inputFile);
	String line;
	while ((line = bufferReader.readLine()) != null)

	{
		String[] fields = line.split("\\,");
		Long src = Long.parseLong(fields[0]);
		Long trg = Long.parseLong(fields[1]);
		if(!T.containsKey(new Tuple2<>(src,trg))) {
			T.put(new Tuple2<>(src,trg),0L);

		}
	}
		for (int i = 1; i <= 4; i++) {
			FileReader inputFile2 = new FileReader("/Users/zainababbas/partitioning/gelly-streaming/GRFR/"+i);

			List<Long> vertices= new ArrayList<>();
			BufferedReader bufferReader2 = new BufferedReader(inputFile2);
			String line2;
			while ((line2 = bufferReader2.readLine()) != null)
			{
				String[] fields = line2.split("\\,");
				Long contains = Long.parseLong(fields[0]);
		vertices.add(contains);

			}
bufferReader2.close();
			for (Tuple2<Long, Long> key : T.keySet()) {
				if(vertices.contains(key.f0) && vertices.contains(key.f1))
				{
					T.put(key,0L);


				}
				else if(!vertices.contains(key.f0) && vertices.contains(key.f1))
				{
					T.put(key,1L);

				}
				else if(vertices.contains(key.f0) && !vertices.contains(key.f1))
				{
					T.put(key,1L);

				}
			/*	else if(!vertices.contains(key.f0) && !vertices.contains(key.f1))
				{
					T.put(key,0L);

				}*/

			}

			}

		long sum=0;
		double edgecut= 0.0;
		for (Tuple2<Long, Long> key : T.keySet()) {
			if(T.get(key)==1L) {
				sum = sum + 1;
			}


		}
		    edgecut= (double) sum/T.size();
		System.out.print(edgecut);
		FileWriter fw = new FileWriter("/Users/zainababbas/partitioning/gelly-streaming/NEWBIG/3/BigS", true); //the true will append the new data
		fw.write("Fennls:"+String.valueOf(edgecut));//appends the string to the file
		fw.write("\n");
		fw.close();

		System.out.print("Done");

	}



//	for (int i = 0; i <= 1; i++)

//	{
		//FileReader inputFile = new FileReader("/Users/zainababbas/partitioning/gelly-streaming/testvertexcut/" + i);
		//Instantiate the BufferedReader Class


//	}


}
