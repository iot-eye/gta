package com.ioteye.gta.flink.mcep.rule;

import com.ioteye.gta.flink.mcep.util.TimeUtils;
import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
public abstract class MCepMeta implements Serializable {
    /** expired **/
    private Boolean closed;
    private Boolean expired;
    /** time **/
    private LocalDateTime startTime;
    private LocalDateTime stopTime;

    public boolean notValid() {
        return !valid();
    }

    public boolean valid() {
        if (Boolean.TRUE.equals(closed) || Boolean.TRUE.equals(expired)) {
            return false;
        }

        LocalDateTime currentLocalTime = TimeUtils.currentLocalTime();
        boolean started = startTime != null && startTime.isAfter(currentLocalTime);
        boolean stoped = stopTime != null && currentLocalTime.isBefore(stopTime);

        if (started && !stoped) {
            return true;
        }

        return false;
    }
}
