package com.ioteye.gta.flink.mcep.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class TimeUtils {
    private static final String CHINA_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private static final ZoneId defaultZoneId = ZoneId.systemDefault();

    //========================= current time =========================//
    public static LocalDateTime currentLocalTime(ZoneId zoneId) {
        return LocalDateTime.now(zoneId);
    }

    public static LocalDateTime currentLocalTime() {
        return currentLocalTime(defaultZoneId);
    }

    //========================= parse time =========================//
    public static LocalDateTime parseLocalTime(String stringTime) {
        return parseLocalTime(CHINA_TIME_PATTERN, stringTime);
    }

    public static LocalDateTime parseLocalTime(String pattern, String stringTime) {
        DateTimeFormatter df = DateTimeFormatter.ofPattern(pattern);
        return LocalDateTime.parse(stringTime, df);
    }
}
