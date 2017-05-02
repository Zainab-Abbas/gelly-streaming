package org.apache.flink.graph.streaming.partitioner.inputgenerator;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;

/**
 * Created by zainababbas on 21/02/2017.
 */
public class outEdges {
	public static void main(String[] args) throws Exception {
		//take list of edges, check for each end vertex if both are in the partition or not, if not mark a 1 in the hashlist, otherwise 0.



		HashMap<Tuple2<Long, Long>, Long> T = new HashMap<>();




		for (int i = 1; i <= 4; i++) {
			FileReader inputFile2 = new FileReader("/Users/zainababbas/partitioning/gelly-streaming/Zainab/Zainab3/2/Fen4/" + i);
			FileWriter fw = new FileWriter("/Users/zainababbas/partitioning/gelly-streaming/Zainab/Zainab3/2/22/Fen4/"+i, true);

			BufferedReader bufferReader2 = new BufferedReader(inputFile2);
			String line2;
			while ((line2 = bufferReader2.readLine()) != null) {
				String[] fields = line2.split("\\,");
				Long contains = Long.parseLong(fields[0]);

				fw.write(String.valueOf(contains));
				fw.write("\n");
			}
			bufferReader2.close();

			fw.close();
		}
		System.out.print("Done");

	}



//	for (int i = 0; i <= 1; i++)

//	{
	//FileReader inputFile = new FileReader("/Users/zainababbas/partitioning/gelly-streaming/testvertexcut/" + i);
	//Instantiate the BufferedReader Class


//	}


}
