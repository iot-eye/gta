package com.ioteye.gta.flink.mcep.rule;

import com.ioteye.gta.flink.mcep.pattern.Pattern;
import com.ioteye.gta.flink.mcep.util.TimeUtils;
import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Set;

@Data
public class RulePattern<IN extends InjectEvent> implements Serializable {
    private static final long serialVersionUID = -2782959101696893392L;

    /** pattern id **/
    private Integer patternId;

    /** expired **/
    private Boolean expired;
    /** expired time **/
    private LocalDateTime expiredTime;

    /** pattern **/
    private Pattern<IN, ?> pattern;

    /** multi event */
    private Set<RuleEvent<IN>> ruleEvents;

    public boolean expired() {
        LocalDateTime currentLocalTime = TimeUtils.currentLocalTime();
        if (Boolean.TRUE.equals(expired) || (expiredTime != null && currentLocalTime.isBefore(expiredTime))) {
            return true;
        }
        return false;
    }

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
