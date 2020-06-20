package com.ioteye.gta.flink.mcep.rule;

import java.io.Serializable;

public interface InjectEvent extends Serializable {
    Integer getEventId();
}
