package com.ioteye.gta.flink.mcep.rule;

import com.ioteye.gta.flink.mcep.pattern.Pattern;
import lombok.Data;

import java.io.Serializable;
import java.util.Set;

@Data
public class RulePattern<IN extends InjectEvent> extends MCepMeta implements Serializable {
    private static final long serialVersionUID = 2782959101696893392L;

    /** pattern id **/
    private Integer patternId;

    /** pattern **/
    private Pattern<IN, ?> pattern;

    /** multi event */
    private Set<RuleEvent<IN>> ruleEvents;


    @Override
    public final int hashCode() {
        if (patternId == null) {
            return 0;
        }
        return patternId.hashCode();
    }

    @Override
    public final boolean equals(Object obj) {
        if (!(obj instanceof RulePattern)) {
            return false;
        }
        Integer otherPatternId = ((RulePattern) obj).patternId;
        if (otherPatternId == null) {
            return false;
        }
        return otherPatternId.equals(patternId);
    }
}
