package com.ioteye.gta.flink.mcep.functions;

import org.apache.flink.api.java.functions.KeySelector;

public abstract class EventSelector<IN, E> implements KeySelector<IN, E> {
    E event;

    public EventSelector(E event) {
        this.event = event;
    }

    @Override
    public final E getKey(IN in) throws Exception {
        return getEvent(in);
    }

    public E getEvent(IN in) throws Exception {
        if (verifyEvent(in)) {
            return event;
        }
        return null;
    }

    public abstract boolean verifyEvent(IN in);

    public E getEvent() {
        return event;
    }

    @Override
    public final int hashCode() {
        return event.hashCode();
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj instanceof EventSelector) {
            Object otherEvent = ((EventSelector) obj).getEvent();
            if (this.event.equals(otherEvent)) {
                return true;
            }
        }

        return false;
    }
}
