package com.ioteye.gta.flink.mcep.pattern.conditions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;

@PublicEvolving
public abstract class TimeIterativeCondition<T> extends IterativeCondition<T> implements Function, Serializable {
	private static final long serialVersionUID = 8067817235759351255L;

	public boolean filter(T event, Context<T> ctx) throws Exception {
		if (timeFilter(event, ctx)) {
			return stepAction(event, ctx);
		}

		return false;
	}

	public abstract int step(T event, Context<T> ctx) throws Exception;

	public abstract int conditionId(T event, Context<T> ctx) throws Exception;

	public abstract boolean timeFilter(T event, Context<T> ctx) throws Exception;

	final boolean stepAction(T event, Context<T> ctx) throws Exception {
		return false;
	}
}
