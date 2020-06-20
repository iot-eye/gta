package com.ioteye.gta.flink.mcep;

import com.ioteye.gta.flink.mcep.rule.InjectEvent;
import com.ioteye.gta.flink.mcep.rule.RulePattern;

import java.io.Serializable;
import java.util.List;

/**
 * dynamic injection pattern function
 * @param <IN>
 */
public interface InjectionPatternFunction<IN extends InjectEvent> extends Serializable {

	/**
     * pattern inject open method
	 * @throws Exception exception
	 */
	void open() throws Exception;


	/**
	 * dynamic load period, unit second
	 * @return period
	 */
	long refreshPeriod();

	/**
     * dynamic inject pattern, [patternId, [pattern, <event>]
	 * @return pattern list struct
	 * @throws Exception exception
	 */
	List<RulePattern<IN>> injectPatterns() throws Exception;
}
