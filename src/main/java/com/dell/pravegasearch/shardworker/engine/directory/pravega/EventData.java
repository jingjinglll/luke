package com.dell.pravegasearch.shardworker.engine.directory.pravega;

import java.io.Serializable;

public class EventData implements Serializable {

    private final String eventName;
    private final long offset;
    private final int length;

    public EventData(String eventName, long offset, int length) {
        this.eventName = eventName;
        this.offset = offset;
        this.length = length;
    }

    public long getOffset() {
        return this.offset;
    }

    public int getLength() {
        return this.length;
    }
}
