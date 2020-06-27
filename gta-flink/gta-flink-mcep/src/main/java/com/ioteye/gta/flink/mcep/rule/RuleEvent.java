package com.ioteye.gta.flink.mcep.rule;

import com.ioteye.gta.flink.mcep.functions.EventSelector;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class RuleEvent<IN extends InjectEvent> extends MCepMeta  implements Serializable {
    private static final long serialVersionUID = -792215188420027391L;

    /** event id **/
    private Integer eventId;

    /** event selector **/
    private EventSelector<IN, RuleEvent<IN>> eventSelector;


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
