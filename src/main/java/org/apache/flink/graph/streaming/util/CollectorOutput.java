package org.apache.flink.graph.streaming.util;



	import org.apache.commons.lang3.SerializationUtils;
	import org.apache.flink.streaming.api.operators.Output;
	import org.apache.flink.streaming.api.watermark.Watermark;
	import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
	import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
	import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

	import java.io.Serializable;
	import java.util.List;

	public class CollectorOutput<T> implements Output<StreamRecord<T>> {

		private final List<StreamElement> list;

		public CollectorOutput(List<StreamElement> list) {
			this.list = list;
		}

		@Override
		public void emitWatermark(Watermark mark) {
			list.add(mark);
		}

		@Override
		public void emitLatencyMarker(LatencyMarker latencyMarker) {
			list.add(latencyMarker);
		}

		@Override
		public void collect(StreamRecord<T> record) {
			T copied = SerializationUtils.deserialize(SerializationUtils.serialize((Serializable) record.getValue()));
			list.add(record.copy(copied));
		}

		@Override
		public void close() {}
	}

