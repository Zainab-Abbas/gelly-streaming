package org.apache.flink.graph.streaming.summaries;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zainababbas on 15/04/2017.
 */
public class HMap implements Serializable {

	private Map<Long, Integer> verticesWithDegrees;
	public HMap() {

		verticesWithDegrees = new HashMap<>();
	}




	public void union(Long e1, Long e2) {

		if (verticesWithDegrees.containsKey(e1)) {
			// update existing vertex

			int oldDegree =verticesWithDegrees.get(e1);
			Integer newDegree = oldDegree + 1;
			if (newDegree > 0) {
				verticesWithDegrees.put(e1, newDegree);

			} else {
				// if the current degree is <= 0: remove the vertex
				verticesWithDegrees.remove(e1);
			}

		} else {


				verticesWithDegrees.put(e1, 1);

		}
		if (verticesWithDegrees.containsKey(e2)) {
			// update existing vertex

			int oldDegree =verticesWithDegrees.get(e2);
			Integer newDegree = oldDegree + 1;
			if (newDegree > 0) {
				verticesWithDegrees.put(e2, newDegree);

			} else {
				// if the current degree is <= 0: remove the vertex
				verticesWithDegrees.remove(e2);
			}

		} else {


			verticesWithDegrees.put(e2, 1);

		}
	}

	public void union1(Long e1, Integer d) {

		if (verticesWithDegrees.containsKey(e1)) {
			// update existing vertex

			int oldDegree =verticesWithDegrees.get(e1);
			Integer newDegree = oldDegree + d;
			if (newDegree > 0) {
				verticesWithDegrees.put(e1, newDegree);

			} else {
				// if the current degree is <= 0: remove the vertex
				verticesWithDegrees.remove(e1);
			}

		} else {


			verticesWithDegrees.put(e1, 1);

		}

	}


	public Map<Long, Integer> getmap (){
		return  verticesWithDegrees;
}
	public void merge(Map<Long, Integer> other) {

		for (Map.Entry<Long, Integer> entry : other.entrySet()) {
			union1(entry.getKey(),entry.getValue());
		}
	}
	public int size() {

		return  verticesWithDegrees.size();
	}
}
