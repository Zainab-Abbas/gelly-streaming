package org.apache.flink.graph.streaming.partitioner.edgepartitioners.batchapp;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.streaming.partitioner.edgepartitioners.keyselector.CustomKeySelector3;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * Created by zainababbas on 09/05/2017.
 */
@SuppressWarnings("serial")
public class ConnectedComponents {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String... args) throws Exception {

		// Checking input parameters
		//final ParameterTool params = ParameterTool.fromArgs(args);

		// set up execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		final int maxIterations = 10;

		// make parameters available in the web interface
		//env.getConfig().setGlobalJobParameters(params);

		// read vertex and edge data
		DataSet<Edge<Long, NullValue>> data = env.readTextFile("/Users/zainababbas/partitioning/gelly-streaming/sample_graph.txt").map(new MapFunction<String, Edge<Long, NullValue>>() {

			@Override
			public Edge<Long, NullValue> map(String s) {
				String[] fields = s.split("\\,");
				long src = Long.parseLong(fields[0]);
				long trg = Long.parseLong(fields[1]);
				return new Edge<>(src, trg, NullValue.getInstance());
			}
		});


		//	DataSet<Edge<Long, NullValue>> partitionedData =
		//			data.partitionCustom(new HashPartitioner<>(new CustomKeySelector(0)), new CustomKeySelector<>(0));


		Graph<Long, Long, NullValue> graph = Graph.fromDataSet(data, new InitVertices(0), env);

		DataSet<Long> vertices = graph.getVertices().map(new MapFunction<Vertex<Long,Long>, Long>() {
			 @Override
			 public Long map(Vertex<Long, Long> longLongVertex) throws Exception {
				 return longLongVertex.f0;
			 }
		 });

		//vertices.print();

		DataSet<Tuple2<Long, Long>> edges =

				graph.getEdges().flatMap(new FlatMapFunction<Edge<Long, NullValue>, Tuple2<Long, Long>>() {

					@Override
					public void flatMap(Edge<Long, NullValue> longNullValueEdge, Collector<Tuple2<Long, Long>> collector) throws Exception {
						Tuple2<Long,Long> t = new Tuple2<Long, Long>();
						t.f0=longNullValueEdge.f0;
						t.f1=longNullValueEdge.f1;
						collector.collect(t);
					}

				});

		//edges.print();



		//DataSet<Long> vertices = getVertexDataSet(env, params);
		//DataSet<Tuple2<Long, Long>> edges = getEdgeDataSet(env, params).flatMap(new UndirectEdge());

		// assign the initial components (equal to the vertex id)
		DataSet<Tuple2<Long, Long>> verticesWithInitialId =
				vertices.map(new DuplicateValue<Long>());

		// open a delta iteration
		DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration =
				verticesWithInitialId.iterateDelta(verticesWithInitialId, maxIterations, 0);


		// apply the step logic: join with the edges, select the minimum neighbor, update if the component of the candidate is smaller
		DataSet<Tuple2<Long, Long>> changes = iteration.getWorkset().join(edges).where(0).equalTo(0).with(new NeighborWithComponentIDJoin())
													  .groupBy(0)
													  .aggregate(Aggregations.MIN, 1).partitionCustom(new HashPartitioner<>(new CustomKeySelector3(0)),new CustomKeySelector3<>(0))
													  .join(iteration.getSolutionSet()).where(0).equalTo(0)
													  .with(new ComponentIdFilter());

		// close the delta iteration (delta and new workset are identical)
		DataSet<Tuple2<Long, Long>> result = iteration.closeWith(changes, changes);
		//result.partitionCustom(new HashPartitioner<>(new CustomKeySelector3(0)),new CustomKeySelector3<>(0));
		//result.partitionCustom(new HashPartitioner<>(new CustomKeySelector3(0)),new CustomKeySelector3<>(0)).print();
		/// / emit result
		//if (params.has("output")) {
		//	result.writeAsCsv(params.get("output"), "\n", " ");
		result.print();
		//vertices.print();
		env.execute("Connected Components Example");
	//	} else {
		//	System.out.println("Printing result to stdout. Use --output to specify output path.");
		//	result.print();
	//	}
	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	/**
	 * Function that turns a value into a 2-tuple where both fields are that value.
	 */
	@FunctionAnnotation.ForwardedFields("*->f0")
	public static final class DuplicateValue<T> implements MapFunction<T, Tuple2<T, T>> {

		@Override
		public Tuple2<T, T> map(T vertex) {
			return new Tuple2<T, T>(vertex, vertex);
		}
	}

	/**
	 * Undirected edges by emitting for each input edge the input edges itself and an inverted version.
	 */
	public static final class UndirectEdge implements FlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
		Tuple2<Long, Long> invertedEdge = new Tuple2<Long, Long>();

		@Override
		public void flatMap(Tuple2<Long, Long> edge, Collector<Tuple2<Long, Long>> out) {
			invertedEdge.f0 = edge.f1;
			invertedEdge.f1 = edge.f0;
			out.collect(edge);
			out.collect(invertedEdge);
		}
	}

	/**
	 * UDF that joins a (Vertex-ID, Component-ID) pair that represents the current component that
	 * a vertex is associated with, with a (Source-Vertex-ID, Target-VertexID) edge. The function
	 * produces a (Target-vertex-ID, Component-ID) pair.
	 */
	@FunctionAnnotation.ForwardedFieldsFirst("f1->f1")
	@FunctionAnnotation.ForwardedFieldsSecond("f1->f0")
	public static final class NeighborWithComponentIDJoin implements JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		@Override
		public Tuple2<Long, Long> join(Tuple2<Long, Long> vertexWithComponent, Tuple2<Long, Long> edge) {
			return new Tuple2<Long, Long>(edge.f1, vertexWithComponent.f1);
		}
	}



	@FunctionAnnotation.ForwardedFieldsFirst("*")
	public static final class ComponentIdFilter implements FlatJoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		@Override
		public void join(Tuple2<Long, Long> candidate, Tuple2<Long, Long> old, Collector<Tuple2<Long, Long>> out) {
			if (candidate.f1 < old.f1) {
				out.collect(candidate);
			}
		}
	}
	private static class HashPartitioner<T> implements Partitioner<T> {
		private static final long serialVersionUID = 1L;
		CustomKeySelector3 keySelector;
		private static final int MAX_SHRINK = 100;
		private double seed;
		private int shrink;
		public HashPartitioner(CustomKeySelector3 keySelector)
		{
			this.keySelector = keySelector;
			System.out.println("createdsfsdfsdfsdf");
			this.seed = Math.random();
			Random r = new Random();
			shrink = r.nextInt(MAX_SHRINK);

		}

		@Override
		public int partition(Object key, int numPartitions) {
			//return MathUtils.murmurHash(key.hashCode()) % numPartitions;
			return Math.abs((int) (  (int) Integer.parseInt(key.toString())*seed*shrink) % numPartitions);


		}

	}

	@SuppressWarnings("serial")
	private static final class InitVertices implements MapFunction<Long, Long>{

		private long srcId;

		public InitVertices(long srcId) {
			this.srcId = srcId;
		}

		public Long map(Long id) {
			if (id.equals(srcId)) {
				return 0L;
			}
			else {
				return Long.MAX_VALUE;
			}
		}
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

/*	private static DataSet<Long> getVertexDataSet(ExecutionEnvironment env, ParameterTool params) {
		if (params.has("vertices")) {
			return env.readCsvFile(params.get("vertices")).types(Long.class).map(
					new MapFunction<Tuple1<Long>, Long>() {
						public Long map(Tuple1<Long> value) {
							return value.f0;
						}
					});
		} else {
			System.out.println("Executing Connected Components example with default vertices data set.");
			System.out.println("Use --vertices to specify file input.");
			return ConnectedComponentsData.getDefaultVertexDataSet(env);
		}
	}

	private static DataSet<Tuple2<Long, Long>> getEdgeDataSet(ExecutionEnvironment env, ParameterTool params) {
		if (params.has("edges")) {
			return env.readCsvFile(params.get("edges")).fieldDelimiter(" ").types(Long.class, Long.class);
		} else {
			System.out.println("Executing Connected Components example with default edges data set.");
			System.out.println("Use --edges to specify file input.");
			return ConnectedComponentsData.getDefaultEdgeDataSet(env);
		}
	}*/


}
