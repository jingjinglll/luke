package com.dell.pravegasearch.shardworker.engine.directory.pravega;

import java.io.Serializable;
import java.util.Map;

public class CommitPoint implements Serializable {

    private final Map<String, EventData> eventDataMap;
    private final String name;
    private final byte[] commitPoint;

    public CommitPoint(Map<String, EventData> eventDataMap, String name, byte[] commitPoint) {
        this.eventDataMap = eventDataMap;
        this.name = name;
        this.commitPoint = commitPoint;
    }

    public Map<String, EventData> getEventDataMap() {
        return eventDataMap;
    }

    public String getName() {
        return name;
    }

    public byte[] getCommitPoint() {
        return commitPoint;
    }

}
