package com.ioteye.gta.flink.mcep;

import com.ioteye.gta.flink.mcep.rule.InjectEvent;
import org.apache.flink.streaming.api.datastream.DataStream;

public class GtaCEP {
	/**
	 * Creates a {@link PatternStream} from an input data stream and a inject pattern function.
	 *
	 * @param input DataStream containing the input events
	 * @param function function specification which shall be detected
	 * @param <T> Type of the input events
	 * @return Resulting pattern stream
	 */
	public static <T extends InjectEvent> PatternStream<T> injectPattern(DataStream<T> input, InjectionPatternFunction<T> function) {
		return new PatternStream<>(input, function);
	}
}
