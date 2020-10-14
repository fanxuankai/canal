package com.fanxuankai.canal.core;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author fanxuankai
 */
public class EntryConsumerFactory {

    private final Map<CanalEntry.EventType, EntryConsumer<?>> consumerMap = new HashMap<>(16);

    public void put(CanalEntry.EventType eventType, EntryConsumer<?> entryConsumer) {
        consumerMap.put(eventType, entryConsumer);
    }

    public Optional<EntryConsumer<?>> find(CanalEntry.EventType eventType) {
        return Optional.ofNullable(consumerMap.get(eventType));
    }

}
