package org.apache.flink.graph.streaming.partitioner.inputgenerator;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zainababbas on 21/02/2017.
 */
public class newedgecuts {
	public static void main(String[] args) throws Exception {
		//take list of edges, check for each end vertex if both are in the partition or not, if not mark a 1 in the hashlist, otherwise 0.

		FileReader inputFile2 = new FileReader("/Users/zainababbas/partitioning/gelly-streaming/youtubeout/"+2);

		List<Long> vertices= new ArrayList<>();
		BufferedReader bufferReader2 = new BufferedReader(inputFile2);
		String line2;
		while ((line2 = bufferReader2.readLine()) != null)
		{

			Long contains = Long.parseLong(line2);
			vertices.add(contains);

		}
		bufferReader2.close();

		System.out.print("step1");
		List<Tuple2<Long, Long>> T = new ArrayList<>();

		FileReader inputFile = new FileReader("/Users/zainababbas/working/gelly-streaming/big/youtube.txt");

		//Instantiate the BufferedReader Class
long sum=0;
		BufferedReader bufferReader = new BufferedReader(inputFile);
		String line;
		while ((line = bufferReader.readLine()) != null)

		{
			String[] fields = line.split("\\t");
			Long src = Long.parseLong(fields[0]);
			Long trg = Long.parseLong(fields[1]);
			if(!vertices.contains(src) && vertices.contains(trg))
			{
				sum++;

			}
			else if(vertices.contains(src) && !vertices.contains(trg))
			{
				sum++;


			}

		}

		//for (int i = 1; i <= 4; i++) {



	//	}



		System.out.print(sum);
		//FileWriter fw = new FileWriter("/Users/zainababbas/partitioning/gelly-streaming/NEWBIG/3/BigS", true); //the true will append the new data
		//fw.write("Fennls:"+String.valueOf(edgecut));//appends the string to the file
		//fw.write("\n");
		//fw.close();

		System.out.print("Done");

	}



//	for (int i = 0; i <= 1; i++)

//	{
	//FileReader inputFile = new FileReader("/Users/zainababbas/partitioning/gelly-streaming/testvertexcut/" + i);
	//Instantiate the BufferedReader Class


//	}


}
