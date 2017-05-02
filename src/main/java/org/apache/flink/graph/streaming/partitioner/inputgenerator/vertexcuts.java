package org.apache.flink.graph.streaming.partitioner.inputgenerator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by zainababbas on 21/02/2017.
 */
public class vertexcuts {

	public static void main(String[] args) throws Exception {


		HashMap<Long, List<Long>> T = new HashMap<>();
		for (int i = 1; i <= 4; i++) {
			FileReader inputFile = new FileReader("/Users/zainababbas/partitioning/gelly-streaming/Zainab/Zainab2/2/DbhD/" + i);
			//Instantiate the BufferedReader Class
//System.out.print("count"+i);
			BufferedReader bufferReader = new BufferedReader(inputFile);
			String line;
			while ((line = bufferReader.readLine()) != null) {
				String[] fields = line.split("\\,");
				Long src = Long.parseLong(fields[0]);
				Long trg = Long.parseLong(fields[1]);
if(!T.containsKey(src)) {
	T.put(src, new ArrayList<>());
}

				if(!T.containsKey(trg))
				{
					T.put(trg,new ArrayList<>());

				}

			}


			bufferReader.close();
			System.out.println("done");
		}

		for (int i = 1; i <= 4; i++) {
			FileReader inputFile = new FileReader("/Users/zainababbas/partitioning/gelly-streaming/Zainab/Zainab2/2/DbhD/" + i);
			//Instantiate the BufferedReader Class
		//	System.out.print("count" + i);
			BufferedReader bufferReader = new BufferedReader(inputFile);
			String line;
			while ((line = bufferReader.readLine()) != null) {
				String[] fields = line.split("\\,");
				Long src = Long.parseLong(fields[0]);
				Long trg = Long.parseLong(fields[1]);

				if (T.containsKey(src)) {
					List<Long> p = T.get(src);
					if(!p.contains((long) i))
					{p.add((long) i);
					T.put(src, p);}

				}
				if (T.containsKey(trg)) {
					List<Long> p = T.get(trg);
					if(!p.contains((long) i))
					{p.add((long) i);
						T.put(trg, p);}

				}


			}
			bufferReader.close();
			System.out.println("done");

		}


		      long sum=0;
		double rep = 0.0;

		for (Long key : T.keySet()) {

			  sum=sum+T.get(key).size();
	}

		rep = (double) sum/T.size();
		     	System.out.println("replication factor:"+rep);

		FileWriter fw = new FileWriter("/Users/zainababbas/partitioning/gelly-streaming/Zainab/Zainab2/2/nMD", true); //the true will append the new data
		fw.write("Dbh:"+String.valueOf(rep));//appends the string to the file
		fw.write("\n");
		fw.close();

}

}

