package com.ioteye.gta.flink.mcep.rule;

import com.ioteye.gta.flink.mcep.functions.EventSelector;
import com.ioteye.gta.flink.mcep.util.TimeUtils;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.time.LocalDateTime;

@Getter
@Setter
public class RuleEvent<IN extends InjectEvent> implements Serializable {
    private static final long serialVersionUID = -792215188420027391L;

    /** event id **/
    private Integer eventId;

    /** expired **/
    private Boolean expired;
    /** expired time **/
    private LocalDateTime expiredTime;

    /** event selector **/
    private EventSelector<IN, RuleEvent<IN>> eventSelector;

    public boolean expired() {
        LocalDateTime currentLocalTime = TimeUtils.currentLocalTime();
        if (Boolean.TRUE.equals(expired) || (expiredTime != null && currentLocalTime.isBefore(expiredTime))) {
            return true;
        }
        return false;
    }

    public RuleEvent() {
        super();
        this.init();
    }

    private void init () {
        this.eventSelector = new EventSelector<IN, RuleEvent<IN>>(this) {
            private static final long serialVersionUID = -792215188420027391L;

            @Override
            public boolean verifyEvent(IN in) {
                if (in != null && getEvent().getEventId().equals(in.getEventId())) {
                    return true;
                }

                return false;
            }
        };
    }

    @Override
    public int hashCode() {
        if (eventId == null) {
            return 0;
        }
        return eventId;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RuleEvent)) {
            return false;
        }
        Integer otherEventId = ((RuleEvent) obj).eventId;
        if (otherEventId == null) {
            return false;
        }
        return otherEventId.equals(eventId);
    }
}
