package com.fanxuankai.canal.elasticsearch.config;

import com.fanxuankai.canal.core.config.ConsumerConfig;
import com.fanxuankai.canal.core.model.EntryWrapper;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * @author fanxuankai
 */
public class CanalElasticsearchConfiguration {

    private Map<String, Map<String, ConsumerConfig>> consumerConfigMap = Collections.emptyMap();

    public Map<String, Map<String, ConsumerConfig>> getConsumerConfigMap() {
        return consumerConfigMap;
    }

    public void setConsumerConfigMap(Map<String, Map<String, ConsumerConfig>> consumerConfigMap) {
        this.consumerConfigMap = consumerConfigMap;
    }

    public Optional<ConsumerConfig> getConsumerConfig(EntryWrapper entryWrapper) {
        return Optional.ofNullable(consumerConfigMap)
                .map(map -> map.get(entryWrapper.getSchemaName()))
                .map(map -> map.get(entryWrapper.getTableName()));
    }

}
