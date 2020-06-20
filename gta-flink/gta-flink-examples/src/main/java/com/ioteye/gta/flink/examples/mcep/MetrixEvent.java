package com.ioteye.gta.flink.examples.mcep;

import com.ioteye.gta.flink.mcep.rule.InjectEvent;
import lombok.Data;

import java.io.Serializable;

@Data
public class MetrixEvent implements InjectEvent, Serializable {
    private String key;
    private String userName;
    private Integer eventId;
    private String ip;
    private String type;
    private long timestamp;
}
