package com.fanxuankai.canal.mq.core.config;

import com.fanxuankai.canal.core.model.EntryWrapper;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * @author fanxuankai
 */
public class CanalMqConfiguration {

    private Map<String, Map<String, MqConsumerConfig>> consumerConfigMap = Collections.emptyMap();

    /**
     * 全局分组
     */
    private String globalGroup;

    public Map<String, Map<String, MqConsumerConfig>> getConsumerConfigMap() {
        return consumerConfigMap;
    }

    public void setConsumerConfigMap(Map<String, Map<String, MqConsumerConfig>> consumerConfigMap) {
        this.consumerConfigMap = consumerConfigMap;
    }

    public String getGlobalGroup() {
        return globalGroup;
    }

    public void setGlobalGroup(String globalGroup) {
        this.globalGroup = globalGroup;
    }

    public String getTopic(EntryWrapper entryWrapper) {
        return getConsumerConfig(entryWrapper).map(MqConsumerConfig::getTopic).orElse(null);
    }

    public String getGroup(EntryWrapper entryWrapper) {
        return getConsumerConfig(entryWrapper).map(MqConsumerConfig::getGroup).orElse(null);
    }

    public Optional<MqConsumerConfig> getConsumerConfig(EntryWrapper entryWrapper) {
        return Optional.ofNullable(consumerConfigMap)
                .map(map -> map.get(entryWrapper.getSchemaName()))
                .map(map -> map.get(entryWrapper.getTableName()));
    }

}
