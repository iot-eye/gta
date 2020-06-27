package com.ioteye.gta.flink.mcep.rule;

import com.ioteye.gta.flink.mcep.functions.EventSelector;
import com.ioteye.gta.flink.mcep.nfa.NFA;
import com.ioteye.gta.flink.mcep.nfa.NFAState;
import com.ioteye.gta.flink.mcep.nfa.aftermatch.AfterMatchSkipStrategy;
import com.ioteye.gta.flink.mcep.nfa.compiler.NFACompiler;
import com.ioteye.gta.flink.mcep.pattern.Pattern;
import com.sun.org.apache.bcel.internal.generic.IF_ACMPEQ;
import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@Setter
public class RuleNFA<IN extends InjectEvent> implements Serializable {
    private static final long serialVersionUID = 478853296330152693L;

    /** pattern to nfa map **/
    private Map<RulePattern<IN>, Tuple3<NFACompiler.NFAFactory<IN>, NFA<IN>, AfterMatchSkipStrategy>> nfaMap = new ConcurrentHashMap<>();

    /** event to pattern map **/
    private Map<RuleEvent<IN>, Set<RulePattern<IN>>> eventPatternRelationMap = new ConcurrentHashMap<>();

    /** event selector **/
    private Map<EventSelector<IN, RuleEvent<IN>>, Set<RulePattern<IN>>> eventSelectorPatternRelationMap = new ConcurrentHashMap<>();

    /** event selector **/
    private Set<EventSelector<IN, RuleEvent<IN>>> eventSelectorSet = new HashSet<>();

    /** cache expired nfa **/
    private Set<NFA<IN>> expiredNfaSet = new HashSet<>();

    /** pattern -> nfa state **/
    private MapState<Integer, NFAState> computeStateMap;

    public RuleNFA() {

    }

    public RuleNFA<IN> cloneNfa() {
        RuleNFA<IN> ruleNFA = new RuleNFA<>();

        ruleNFA.nfaMap.putAll(this.nfaMap);
        ruleNFA.expiredNfaSet.addAll(this.expiredNfaSet);
        ruleNFA.eventPatternRelationMap.putAll(this.eventPatternRelationMap);
        ruleNFA.eventSelectorPatternRelationMap.putAll(this.eventSelectorPatternRelationMap);

        ruleNFA.computeStateMap = this.computeStateMap;
        ruleNFA.eventSelectorSet = this.eventSelectorSet;

        return ruleNFA;
    }

    public Tuple3<NFACompiler.NFAFactory<IN>, NFA<IN>, AfterMatchSkipStrategy> getNfaMetaIfAbsent(RulePattern<IN> rulePattern) {
        return nfaMap.computeIfAbsent(rulePattern, e -> new Tuple3<>());
    }

    public NFA<IN> generateNfaMeta(RulePattern<IN> rulePattern, boolean timeoutHandling) {
        Tuple3<NFACompiler.NFAFactory<IN>, NFA<IN>, AfterMatchSkipStrategy> nfaMeta = getNfaMetaIfAbsent(rulePattern);
        Pattern<IN, ?> pattern = rulePattern.getPattern();

        // create
        final NFACompiler.NFAFactory<IN> nfaFactory = NFACompiler.compileFactory(pattern, timeoutHandling);

        NFA<IN> nfa = nfaFactory.createNFA();
        AfterMatchSkipStrategy afterMatchSkipStrategy =
                pattern.getAfterMatchSkipStrategy() == null ? AfterMatchSkipStrategy.noSkip() : pattern.getAfterMatchSkipStrategy();

        nfaMeta.setFields(nfaFactory, nfa, afterMatchSkipStrategy);

        return nfa;
    }

    public void refreshRelationMap(RulePattern<IN> rulePattern) {
        //========================= event pattern ============================//
        Set<RuleEvent<IN>> ruleEventSet = rulePattern.getRuleEvents();

        for(RuleEvent<IN> ruleEvent: ruleEventSet) {
            if (ruleEvent.notValid()) {
                continue;
            }
            Set<RulePattern<IN>> eventPatternSet = eventPatternRelationMap.computeIfAbsent(ruleEvent, e -> new HashSet<>());
            eventPatternSet.add(rulePattern);

            //========================= event selector ============================//
            EventSelector<IN, RuleEvent<IN>> eventSelector = ruleEvent.getEventSelector();
            Set<RulePattern<IN>> rulePatternSet = eventSelectorPatternRelationMap.computeIfAbsent(eventSelector, e -> new HashSet<>());
            rulePatternSet.add(rulePattern);

            eventSelectorSet.add(eventSelector);
        }
    }

    public void removeExpiredPattern(RulePattern<IN> rulePattern) throws Exception {
        if (rulePattern.notValid()) {
            Integer patternId = rulePattern.getPatternId();
            if (computeStateMap != null) {
                computeStateMap.remove(patternId);
            }

            Tuple3<NFACompiler.NFAFactory<IN>, NFA<IN>, AfterMatchSkipStrategy> nfaMeta = nfaMap.remove(rulePattern);
            if (nfaMeta != null) {
                NFA<IN> nfa = nfaMeta.f1;
                expiredNfaSet.add(nfa);
            }
        }

        Set<RuleEvent<IN>> ruleEventSet = rulePattern.getRuleEvents();
        for(RuleEvent<IN> ruleEvent: ruleEventSet) {
            if (ruleEvent.notValid()) {
                EventSelector<IN, RuleEvent<IN>> eventSelector = ruleEvent.getEventSelector();
                Set<RulePattern<IN>> eventSelectorPatternSet = eventSelectorPatternRelationMap.get(eventSelector);
                if (eventSelectorPatternSet != null) {
                    eventSelectorPatternSet.remove(rulePattern);

                    if (eventSelectorPatternSet.isEmpty()) {
                        eventSelectorPatternRelationMap.remove(eventSelector);
                        eventSelectorSet.remove(eventSelector);
                    }
                }

                Set<RulePattern<IN>> eventPatternSet = eventPatternRelationMap.get(ruleEvent);
                if (eventPatternSet != null) {
                    eventPatternSet.remove(rulePattern);

                    if (eventPatternSet.isEmpty()) {
                        eventPatternRelationMap.remove(ruleEvent);
                    }
                }
            }
        }
    }

    public void checkoutExpireAll () {
        Iterator<Map.Entry<RulePattern<IN>, Tuple3<NFACompiler.NFAFactory<IN>, NFA<IN>, AfterMatchSkipStrategy>>> iter1 = nfaMap.entrySet().iterator();
        while (iter1.hasNext()) {
            Map.Entry<RulePattern<IN>, Tuple3<NFACompiler.NFAFactory<IN>, NFA<IN>, AfterMatchSkipStrategy>> entry = iter1.next();
            RulePattern<IN> rulePattern = entry.getKey();
            if(rulePattern == null || rulePattern.notValid()) {
                iter1.remove();
            }
        }

        Iterator<Map.Entry<RuleEvent<IN>, Set<RulePattern<IN>>>> iter2 = eventPatternRelationMap.entrySet().iterator();
        while (iter2.hasNext()) {
            Map.Entry<RuleEvent<IN>, Set<RulePattern<IN>>> entry = iter2.next();
            RuleEvent<IN> ruleEvent = entry.getKey();
            if (ruleEvent == null) {
                iter2.remove();
                continue;
            }

            if (ruleEvent.notValid()) {
                iter2.remove();

                EventSelector<IN, RuleEvent<IN>> eventSelector = ruleEvent.getEventSelector();
                if (eventSelector != null) {
                    eventSelectorPatternRelationMap.remove(eventSelector);
                    eventSelectorSet.remove(eventSelector);
                }
            }
        }

    }

    public NFAState getOrGenerateNfaState(RulePattern<IN> rulePattern) throws Exception {
        Integer patternId = rulePattern.getPatternId();
        NFAState nfaState = computeStateMap.get(patternId);

        Tuple3<NFACompiler.NFAFactory<IN>, NFA<IN>, AfterMatchSkipStrategy> nfaMeta = getNfaMetaIfAbsent(rulePattern);
        if (nfaState == null && nfaMeta.f1 != null) {
            nfaState = nfaMeta.f1.createInitialNFAState();
            computeStateMap.put(patternId, nfaState);
        }

        return nfaState;
    }

    public NFAState getNfaState(RulePattern<IN> rulePattern) throws Exception {
        Integer patternId = rulePattern.getPatternId();
        return computeStateMap.get(patternId);
    }

    public void refreshNfaSate (RulePattern<IN> rulePattern, NFAState nfaState) throws Exception {
        Integer patternId = rulePattern.getPatternId();

        if (nfaState.isStateChanged()) {
            nfaState.resetStateChanged();
            computeStateMap.put(patternId, nfaState);
        }
    }

    public Map<RulePattern<IN>, NFAState> nfaStateMap () throws Exception {
        Map<RulePattern<IN>, NFAState> nfaStateMap = new HashMap<>();

        for(RulePattern<IN> rulePattern: nfaMap.keySet()) {
            Integer patternId = rulePattern.getPatternId();
            NFAState nfaState = computeStateMap.get(patternId);
            if (nfaState != null) {
                nfaStateMap.put(rulePattern, nfaState);
            }
        }
        return nfaStateMap;
    }

    public void close() throws Exception {
        for(Tuple3<NFACompiler.NFAFactory<IN>, NFA<IN>, AfterMatchSkipStrategy> nfaMeta: nfaMap.values()) {
            if (nfaMeta.f1 != null) {
                NFA<IN> nfa = nfaMeta.f1;
                nfa.close();
            }
        }
    }
}
