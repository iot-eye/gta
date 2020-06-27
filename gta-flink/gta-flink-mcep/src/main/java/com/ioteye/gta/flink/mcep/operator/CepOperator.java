/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ioteye.gta.flink.mcep.operator;

import com.ioteye.gta.flink.mcep.EventComparator;
import com.ioteye.gta.flink.mcep.InjectionPatternFunction;
import com.ioteye.gta.flink.mcep.functions.EventSelector;
import com.ioteye.gta.flink.mcep.functions.PatternProcessFunction;
import com.ioteye.gta.flink.mcep.functions.TimedOutPartialMatchHandler;
import com.ioteye.gta.flink.mcep.nfa.NFA;
import com.ioteye.gta.flink.mcep.nfa.NFA.MigratedNFA;
import com.ioteye.gta.flink.mcep.nfa.NFAState;
import com.ioteye.gta.flink.mcep.nfa.NFAStateSerializer;
import com.ioteye.gta.flink.mcep.nfa.aftermatch.AfterMatchSkipStrategy;
import com.ioteye.gta.flink.mcep.nfa.compiler.NFACompiler;
import com.ioteye.gta.flink.mcep.nfa.sharedbuffer.SharedBuffer;
import com.ioteye.gta.flink.mcep.nfa.sharedbuffer.SharedBufferAccessor;
import com.ioteye.gta.flink.mcep.rule.InjectEvent;
import com.ioteye.gta.flink.mcep.rule.RuleEvent;
import com.ioteye.gta.flink.mcep.rule.RuleNFA;
import com.ioteye.gta.flink.mcep.rule.RulePattern;
import com.ioteye.gta.flink.mcep.time.TimerService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * GtaCEP pattern operator for a keyed input stream. For each key, the operator creates
 * a {@link NFA} and a priority queue to buffer out of order elements. Both data structures are
 * stored using the managed keyed state.
 *
 * @param <IN> Type of the input elements
 * @param <KEY> Type of the key on which the input stream is keyed
 * @param <OUT> Type of the output elements
 */
@Internal
@Slf4j
public class CepOperator<IN extends InjectEvent, KEY, OUT>
		extends AbstractUdfStreamOperator<OUT, PatternProcessFunction<IN, OUT>>
		implements OneInputStreamOperator<IN, OUT>, Triggerable<KEY, VoidNamespace> {

	private static final long serialVersionUID = -4166778210774160757L;

	private final boolean isProcessingTime;

	private final TypeSerializer<IN> inputSerializer;

	///////////////			State			//////////////

	private static final String NFA_STATE_NAME = "nfaStateName";
	private static final String EVENT_QUEUE_STATE_NAME = "eventQueuesStateName";

	private transient MapState<Long, List<IN>> elementQueueState;
	private transient SharedBuffer<IN> partialMatches;

	private transient InternalTimerService<VoidNamespace> timerService;

	/**
	 * The last seen watermark. This will be used to
	 * decide if an incoming element is late or not.
	 */
	private long lastWatermark;

	/** Comparator for secondary sorting. Primary sorting is always done on time. */
	private final EventComparator<IN> comparator;

	/**
	 * {@link OutputTag} to use for late arriving events. Elements with timestamp smaller than
	 * the current watermark will be emitted to this.
	 */
	private final OutputTag<IN> lateDataOutputTag;

	/** Context passed to user function. */
	private transient ContextFunctionImpl context;

	/** Main output collector, that sets a proper timestamp to the StreamRecord. */
	private transient TimestampedCollector<OUT> collector;

	/** Wrapped RuntimeContext that limits the underlying context features. */
	private transient CepRuntimeContext cepRuntimeContext;

	/** Thin context passed to NFA that gives access to time related characteristics. */
	private transient TimerService cepTimerService;

	///////////////			function			//////////////
	/** injection faction **/
	private final InjectionPatternFunction<IN> injectionFunction;

	/** pattern process function **/
	private final PatternProcessFunction<IN, OUT> patternProcessFunction;

	/** gta nfa **/
	private volatile RuleNFA<IN> ruleNFA;

	/** scheduled executor service **/
	private transient ScheduledExecutorService scheduledExecutorService;

	public CepOperator(
			final TypeSerializer<IN> inputSerializer,
			final boolean isProcessingTime,
			@Nullable final EventComparator<IN> comparator,
			final PatternProcessFunction<IN, OUT> patternProcessFunction,
			final InjectionPatternFunction<IN> injectionFunction,
			@Nullable final OutputTag<IN> lateDataOutputTag) {
		super(patternProcessFunction);

		this.inputSerializer = Preconditions.checkNotNull(inputSerializer);

		// function
		this.patternProcessFunction = Preconditions.checkNotNull(patternProcessFunction);
		this.injectionFunction = Preconditions.checkNotNull(injectionFunction);

		this.isProcessingTime = isProcessingTime;
		this.comparator = comparator;
		this.lateDataOutputTag = lateDataOutputTag;

		this.ruleNFA = new RuleNFA<>();
	}

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {
		super.setup(containingTask, config, output);
		this.cepRuntimeContext = new CepRuntimeContext(getRuntimeContext());
		FunctionUtils.setFunctionRuntimeContext(getUserFunction(), this.cepRuntimeContext);
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);

		MapState<Integer, NFAState> computationStates = context.getKeyedStateStore().getMapState(
				new MapStateDescriptor<>(
						NFA_STATE_NAME,
						IntSerializer.INSTANCE,
						new NFAStateSerializer()
				)
		);
		ruleNFA.setComputeStateMap(computationStates);

		partialMatches = new SharedBuffer<>(context.getKeyedStateStore(), inputSerializer);

		elementQueueState = context.getKeyedStateStore().getMapState(
				new MapStateDescriptor<>(
						EVENT_QUEUE_STATE_NAME,
						LongSerializer.INSTANCE,
						new ListSerializer<>(inputSerializer)));

		migrateOldState();
	}

	private void migrateOldState() throws Exception {
		getKeyedStateBackend().applyToAllKeys(
			VoidNamespace.INSTANCE,
			VoidNamespaceSerializer.INSTANCE,
			new MapStateDescriptor<>(
					"nfaOperatorStateName",
					IntSerializer.INSTANCE,
					new NFA.NFASerializer<>(inputSerializer)
			),
			new KeyedStateFunction<Object, MapState<Integer, MigratedNFA<IN>>>() {
				@Override
				public void process(Object o, MapState<Integer, MigratedNFA<IN>> state) throws Exception {
					Iterable<Integer> patternIdIter = state.keys();

					for (Integer patternId : patternIdIter) {
						MigratedNFA<IN> oldState = state.get(patternId);
						com.ioteye.gta.flink.mcep.nfa.SharedBuffer<IN> sharedBuffer = oldState.getSharedBuffer();

						ruleNFA.getComputeStateMap().put(patternId, new NFAState(oldState.getComputationStates()));
						partialMatches.init(sharedBuffer.getEventsBuffer(), sharedBuffer.getPages());
					}
					state.clear();
				}
			}
		);
	}

	@Override
	public void open() throws Exception {
		super.open();
		injectionFunction.open();

		timerService = getInternalTimerService(
				"watermark-callbacks",
				VoidNamespaceSerializer.INSTANCE,
				this);

		// thread pool
		scheduledExecutorService = Executors.newScheduledThreadPool(2);

		// refresh
		long refreshPeriod = injectionFunction.refreshPeriod();
		if (refreshPeriod > 5) {
			long initialDelay = 0;
			scheduledExecutorService.scheduleAtFixedRate(() -> refreshPattern(), initialDelay, refreshPeriod, TimeUnit.SECONDS);
			Runtime.getRuntime().addShutdownHook(new Thread(() -> scheduledExecutorService.shutdown()));
		} else {
			refreshPattern();
		}

		context = new ContextFunctionImpl();
		collector = new TimestampedCollector<>(output);
		cepTimerService = new TimerServiceImpl();
	}

	private void refreshPattern() {
		try {
			RuleNFA<IN> ruleNFA = this.ruleNFA.cloneNfa();
			List<RulePattern<IN>> rulePatternList = injectionFunction.injectPatterns();
			for(RulePattern<IN> rulePattern: rulePatternList) {
				// inject
				refreshNFA(ruleNFA, rulePattern);

				// remove
				ruleNFA.removeExpiredPattern(rulePattern);
			}

			// checkout expire element
			ruleNFA.checkoutExpireAll();

			Set<NFA<IN>> expiredNfaSet = new HashSet<>(ruleNFA.getExpiredNfaSet());
			scheduledExecutorService.schedule(new Thread(() -> closeExpiredNfa(expiredNfaSet)), 10, TimeUnit.SECONDS);

			this.ruleNFA = ruleNFA;
		} catch (Exception e) {
			System.out.println("dynamic refresh pattern error" + e);
			throw new RuntimeException("dynamic refresh pattern error", e);
		}
	}

	private void closeExpiredNfa(Set<NFA<IN>> expiredNfaSet) {
		for(NFA<IN> nfa: expiredNfaSet) {
			try {
				nfa.close();
			} catch (Exception e) {
				System.out.println("close expired nfa error" + e);
			}
		}
	}


	/**
	 * refresh nfa
	 * @param ruleNfa rule nfa
	 * @param rulePattern rule pattern
	 * @throws Exception
	 */
	private void refreshNFA(RuleNFA<IN> ruleNfa, RulePattern<IN> rulePattern) throws Exception {
		// timeout flag
		final boolean timeoutHandling = patternProcessFunction instanceof TimedOutPartialMatchHandler;

		// create
		NFA<IN> nfa = ruleNfa.generateNfaMeta(rulePattern, timeoutHandling);
		nfa.open(cepRuntimeContext, new Configuration());

		ruleNfa.refreshRelationMap(rulePattern);
	}

	@Override
	public void close() throws Exception {
		super.close();
		ruleNFA.close();
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		IN inValue = element.getValue();

		if (isProcessingTime) {
			if (comparator == null) {
				Set<EventSelector<IN, RuleEvent<IN>>> eventSelectorSet = ruleNFA.getEventSelectorSet();
				Map<RuleEvent<IN>, Set<RulePattern<IN>>> eventPatternRelationMap = ruleNFA.getEventPatternRelationMap();

				for(EventSelector<IN, RuleEvent<IN>> eventSelector: eventSelectorSet) {
					RuleEvent<IN> ruleEvent = eventSelector.getEvent(inValue);

					if (ruleEvent != null && eventPatternRelationMap.containsKey(ruleEvent)) {
						Set<RulePattern<IN>> rulePatternSet = eventPatternRelationMap.get(ruleEvent);
						for(RulePattern<IN> rulePattern: rulePatternSet) {

							NFAState nfaState = ruleNFA.getOrGenerateNfaState(rulePattern);

							if (nfaState != null) {
								long timestamp = getProcessingTimeService().getCurrentProcessingTime();
								advanceTime(rulePattern, nfaState, timestamp);
								processEvent(rulePattern, nfaState, element.getValue(), timestamp);
								ruleNFA.refreshNfaSate(rulePattern, nfaState);
							}
						}
					}
				}
			} else {
				long currentTime = timerService.currentProcessingTime();
				bufferEvent(element.getValue(), currentTime);

				// register a timer for the next millisecond to sort and emit buffered data
				timerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, currentTime + 1);
			}

		} else {

			long timestamp = element.getTimestamp();

			// In event-time processing we assume correctness of the watermark.
			// Events with timestamp smaller than or equal with the last seen watermark are considered late.
			// Late events are put in a dedicated side output, if the user has specified one.

			if (timestamp > lastWatermark) {

				// we have an event with a valid timestamp, so
				// we buffer it until we receive the proper watermark.

				saveRegisterWatermarkTimer();

				bufferEvent(inValue, timestamp);

			} else if (lateDataOutputTag != null) {
				output.collect(lateDataOutputTag, element);
			}
		}
	}

	/**
	 * Registers a timer for {@code current watermark + 1}, this means that we get triggered
	 * whenever the watermark advances, which is what we want for working off the queue of
	 * buffered elements.
	 */
	private void saveRegisterWatermarkTimer() {
		long currentWatermark = timerService.currentWatermark();
		// protect against overflow
		if (currentWatermark + 1 > currentWatermark) {
			timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, currentWatermark + 1);
		}
	}

	private void bufferEvent(IN event, long currentTime) throws Exception {
		List<IN> elementsForTimestamp =  elementQueueState.get(currentTime);
		if (elementsForTimestamp == null) {
			elementsForTimestamp = new ArrayList<>();
		}

		if (getExecutionConfig().isObjectReuseEnabled()) {
			// copy the StreamRecord so that it cannot be changed
			elementsForTimestamp.add(inputSerializer.copy(event));
		} else {
			elementsForTimestamp.add(event);
		}
		elementQueueState.put(currentTime, elementsForTimestamp);
	}

	@Override
	public void onEventTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {

		// 1) get the queue of pending elements for the key and the corresponding NFA,
		// 2) process the pending elements in event time order and custom comparator if exists
		//		by feeding them in the NFA
		// 3) advance the time to the current watermark, so that expired patterns are discarded.
		// 4) update the stored state for the key, by only storing the new NFA and MapState iff they
		//		have state to be used later.
		// 5) update the last seen watermark.

		PriorityQueue<Long> sortedTimestamps = getSortedTimestamps();

		// STEP 1
		Map<RulePattern<IN>, NFAState> nfaStateMap = ruleNFA.nfaStateMap();

		Set<RulePattern<IN>> allRulePatternSet = new HashSet<>();

		// STEP 2
		Set<EventSelector<IN, RuleEvent<IN>>> eventSelectorSet = ruleNFA.getEventSelectorSet();
		Map<RuleEvent<IN>, Set<RulePattern<IN>>> eventPatternRelationMap = ruleNFA.getEventPatternRelationMap();

		while (!sortedTimestamps.isEmpty() && sortedTimestamps.peek() <= timerService.currentWatermark()) {
			long timestamp = sortedTimestamps.poll();

			try (Stream<IN> elements = sort(elementQueueState.get(timestamp))) {
				elements.forEachOrdered(
					event -> {
						try {
							for(EventSelector<IN, RuleEvent<IN>> eventSelector: eventSelectorSet) {
								RuleEvent ruleEvent = eventSelector.getEvent(event);
								if (eventPatternRelationMap.containsKey(ruleEvent)) {
									Set<RulePattern<IN>> rulePatternSet = eventPatternRelationMap.get(ruleEvent);
									for(RulePattern<IN> rulePattern: rulePatternSet) {
										NFAState nfaState = ruleNFA.getNfaState(rulePattern);
										advanceTime(rulePattern, nfaState, timestamp);
										processEvent(rulePattern, nfaState, event, timestamp);
									}
									allRulePatternSet.addAll(rulePatternSet);
								}
							}
						} catch (Exception e) {
							throw new RuntimeException(e);
						}
					}
				);
			}
			elementQueueState.remove(timestamp);
		}

		for(RulePattern<IN> rulePattern: allRulePatternSet) {
			NFAState nfaState = nfaStateMap.get(rulePattern);
			// STEP 3
			advanceTime(rulePattern, nfaState, timerService.currentWatermark());

			// STEP 4
			ruleNFA.refreshNfaSate(rulePattern, nfaState);
		}


		if (!sortedTimestamps.isEmpty() || !partialMatches.isEmpty()) {
			saveRegisterWatermarkTimer();
		}

		// STEP 5
		updateLastSeenWatermark(timerService.currentWatermark());
	}

	@Override
	public void onProcessingTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {
		// 1) get the queue of pending elements for the key and the corresponding NFA,
		// 2) process the pending elements in process time order and custom comparator if exists
		//		by feeding them in the NFA
		// 3) update the stored state for the key, by only storing the new NFA and MapState iff they
		//		have state to be used later.

		// STEP 1
		PriorityQueue<Long> sortedTimestamps = getSortedTimestamps();

		Map<RulePattern<IN>, NFAState> nfaStateMap = ruleNFA.nfaStateMap();

		// STEP 2
		Set<EventSelector<IN, RuleEvent<IN>>> eventSelectorSet = ruleNFA.getEventSelectorSet();
		Map<RuleEvent<IN>, Set<RulePattern<IN>>> eventPatternRelationMap = ruleNFA.getEventPatternRelationMap();

		Set<RulePattern<IN>> allRulePatternSet = new HashSet<>();

		while (!sortedTimestamps.isEmpty()) {
			long timestamp = sortedTimestamps.poll();

			try (Stream<IN> elements = sort(elementQueueState.get(timestamp))) {
				elements.forEachOrdered(
					event -> {
						try {
							for(EventSelector<IN, RuleEvent<IN>> eventSelector: eventSelectorSet) {
								RuleEvent<IN> ruleEvent = eventSelector.getEvent(event);
								if (eventPatternRelationMap.containsKey(ruleEvent)) {
									Set<RulePattern<IN>> rulePatternSet = eventPatternRelationMap.get(ruleEvent);

									for(RulePattern<IN> rulePattern: rulePatternSet) {
										NFAState nfaState = nfaStateMap.get(rulePattern);
										advanceTime(rulePattern, nfaState, timestamp);
										processEvent(rulePattern, nfaState, event, timestamp);
									}

									allRulePatternSet.addAll(rulePatternSet);
								}
							}
						} catch (Exception e) {
							throw new RuntimeException(e);
						}
					}
				);
			}
			elementQueueState.remove(timestamp);
		}

		for(RulePattern<IN> rulePattern: allRulePatternSet) {
			NFAState nfaState = nfaStateMap.get(rulePattern);
			// STEP 3
			ruleNFA.refreshNfaSate(rulePattern, nfaState);
		}
	}

	private Stream<IN> sort(Collection<IN> elements) {
		Stream<IN> stream = elements.stream();
		return (comparator == null) ? stream : stream.sorted(comparator);
	}

	private void updateLastSeenWatermark(long timestamp) {
		this.lastWatermark = timestamp;
	}

	private PriorityQueue<Long> getSortedTimestamps() throws Exception {
		PriorityQueue<Long> sortedTimestamps = new PriorityQueue<>();
		for (Long timestamp : elementQueueState.keys()) {
			sortedTimestamps.offer(timestamp);
		}
		return sortedTimestamps;
	}

	/**
	 * Process the given event by giving it to the NFA and outputting the produced set of matched
	 * event sequences.
	 *
	 * @param nfaState Our NFAState object
	 * @param event The current event to be processed
	 * @param timestamp The timestamp of the event
	 */
	private void processEvent(RulePattern<IN> rulePattern, NFAState nfaState, IN event, long timestamp) throws Exception {
		Map<RulePattern<IN>, Tuple3<NFACompiler.NFAFactory<IN>, NFA<IN>, AfterMatchSkipStrategy>> nfaMap = ruleNFA.getNfaMap();
		Tuple3<NFACompiler.NFAFactory<IN>, NFA<IN>, AfterMatchSkipStrategy> nfaTuple3 = nfaMap.get(rulePattern);

		try (SharedBufferAccessor<IN> sharedBufferAccessor = partialMatches.getAccessor()) {
			NFA<IN> nfa = nfaTuple3.f1;
			AfterMatchSkipStrategy afterMatchSkipStrategy = nfaTuple3.f2;

			Collection<Map<String, List<IN>>> patterns =
				nfa.process(sharedBufferAccessor, nfaState, event, timestamp, afterMatchSkipStrategy, cepTimerService);
			processMatchedSequences(patterns, timestamp);
		}
	}

	/**
	 * Advances the time for the given NFA to the given timestamp. This means that no more events with timestamp
	 * <b>lower</b> than the given timestamp should be passed to the nfa, This can lead to pruning and timeouts.
	 */
	private void advanceTime(RulePattern<IN> rulePattern, NFAState nfaState, long timestamp) throws Exception {
		Map<RulePattern<IN>, Tuple3<NFACompiler.NFAFactory<IN>, NFA<IN>, AfterMatchSkipStrategy>> nfaMap = ruleNFA.getNfaMap();

		try (SharedBufferAccessor<IN> sharedBufferAccessor = partialMatches.getAccessor()) {
			Tuple3<NFACompiler.NFAFactory<IN>, NFA<IN>, AfterMatchSkipStrategy> nfaTuple3 = nfaMap.get(rulePattern);
			NFA<IN> nfa = nfaTuple3.f1;
			Collection<Tuple2<Map<String, List<IN>>, Long>> timedOut = nfa.advanceTime(sharedBufferAccessor, nfaState, timestamp);
			if (!timedOut.isEmpty()) {
				processTimedOutSequences(timedOut);
			}
		}
	}

	private void processMatchedSequences(Iterable<Map<String, List<IN>>> matchingSequences, long timestamp) throws Exception {
		PatternProcessFunction<IN, OUT> function = getUserFunction();
		setTimestamp(timestamp);
		for (Map<String, List<IN>> matchingSequence : matchingSequences) {
			function.processMatch(matchingSequence, context, collector);
		}
	}

	private void processTimedOutSequences(Collection<Tuple2<Map<String, List<IN>>, Long>> timedOutSequences) throws Exception {
		PatternProcessFunction<IN, OUT> function = getUserFunction();
		if (function instanceof TimedOutPartialMatchHandler) {

			@SuppressWarnings("unchecked")
			TimedOutPartialMatchHandler<IN> timeoutHandler = (TimedOutPartialMatchHandler<IN>) function;

			for (Tuple2<Map<String, List<IN>>, Long> matchingSequence : timedOutSequences) {
				setTimestamp(matchingSequence.f1);
				timeoutHandler.processTimedOutMatch(matchingSequence.f0, context);
			}
		}
	}

	private void setTimestamp(long timestamp) {
		if (!isProcessingTime) {
			collector.setAbsoluteTimestamp(timestamp);
		}

		context.setTimestamp(timestamp);
	}

	/**
	 * Gives {@link NFA} access to {@link InternalTimerService} and tells if {@link CepOperator} works in
	 * processing time. Should be instantiated once per operator.
	 */
	private class TimerServiceImpl implements TimerService {

		@Override
		public long currentProcessingTime() {
			return timerService.currentProcessingTime();
		}

	}

	/**
	 * Implementation of {@link PatternProcessFunction.Context}. Design to be instantiated once per operator.
	 * It serves three methods:
	 *  <ul>
	 *      <li>gives access to currentProcessingTime through {@link InternalTimerService}</li>
	 *      <li>gives access to timestamp of current record (or null if Processing time)</li>
	 *      <li>enables side outputs with proper timestamp of StreamRecord handling based on either Processing or
	 *          InjectEvent time</li>
	 *  </ul>
	 */
	private class ContextFunctionImpl implements PatternProcessFunction.Context {

		private Long timestamp;

		@Override
		public <X> void output(final OutputTag<X> outputTag, final X value) {
			final StreamRecord<X> record;
			if (isProcessingTime) {
				record = new StreamRecord<>(value);
			} else {
				record = new StreamRecord<>(value, timestamp());
			}
			output.collect(outputTag, record);
		}

		void setTimestamp(long timestamp) {
			this.timestamp = timestamp;
		}

		@Override
		public long timestamp() {
			return timestamp;
		}

		@Override
		public long currentProcessingTime() {
			return timerService.currentProcessingTime();
		}
	}

	//////////////////////			Testing Methods			//////////////////////

	@VisibleForTesting
	boolean hasNonEmptySharedBuffer(KEY key) throws Exception {
		setCurrentKey(key);
		return !partialMatches.isEmpty();
	}

	@VisibleForTesting
	boolean hasNonEmptyPQ(KEY key) throws Exception {
		setCurrentKey(key);
		return elementQueueState.keys().iterator().hasNext();
	}

	@VisibleForTesting
	int getPQSize(KEY key) throws Exception {
		setCurrentKey(key);
		int counter = 0;
		for (List<IN> elements: elementQueueState.values()) {
			counter += elements.size();
		}
		return counter;
	}
}
