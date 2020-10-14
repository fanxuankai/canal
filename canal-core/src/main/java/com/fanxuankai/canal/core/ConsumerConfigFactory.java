package com.fanxuankai.canal.core;

import com.fanxuankai.canal.core.config.ConsumerConfig;
import com.fanxuankai.canal.core.model.EntryWrapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author fanxuankai
 */
public class ConsumerConfigFactory {

    private final Map<String, Map<String, ConsumerConfig>> consumeConfigMap = new HashMap<>(16);

    public void put(String schema, String table, ConsumerConfig consumerConfig) {
        consumeConfigMap.computeIfAbsent(schema, s -> new HashMap<>(16))
                .put(table, consumerConfig);
    }

    public Optional<ConsumerConfig> get(String schema, String table) {
        return Optional.ofNullable(consumeConfigMap.get(schema)).map(o -> o.get(table));
    }

    public Optional<ConsumerConfig> get(EntryWrapper entryWrapper) {
        return get(entryWrapper.getSchemaName(), entryWrapper.getTableName());
    }
}
