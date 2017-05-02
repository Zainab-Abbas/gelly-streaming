package org.apache.flink.graph.streaming.partitioner.edgepartitioners;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Created by zainababbas on 16/02/2017.
 */
public class TimeStampingSource implements ParallelSourceFunction<Tuple2<Long, Long>> {

	private static final long serialVersionUID = -151782334777482511L;

	private volatile boolean running = true;


	@Override
	public void run(SourceFunction.SourceContext<Tuple2<Long, Long>> ctx) throws Exception {

		long num = 100;
		long counter = (long) (Math.random() * 4096);

		while (running) {
			if (num < 100) {
				num++;
				ctx.collect(new Tuple2<Long, Long>(counter++, 0L));
			}
			else {
				num = 0;
				ctx.collect(new Tuple2<Long, Long>(counter++, System.currentTimeMillis()));
			}

			Thread.sleep(1);
		}
	}

	@Override
	public void cancel() {
		running = false;
	}
}