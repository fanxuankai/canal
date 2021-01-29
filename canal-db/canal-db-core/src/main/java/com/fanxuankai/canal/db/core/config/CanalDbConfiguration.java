package com.fanxuankai.canal.db.core.config;

import com.fanxuankai.canal.core.model.EntryWrapper;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author fanxuankai
 */
public class CanalDbConfiguration {

    private Map<String, Map<String, DbConsumerConfig>> consumerConfigMap = Collections.emptyMap();

    public Map<String, Map<String, DbConsumerConfig>> getConsumerConfigMap() {
        return consumerConfigMap;
    }

    public void setConsumerConfigMap(Map<String, Map<String, DbConsumerConfig>> consumerConfigMap) {
        this.consumerConfigMap = consumerConfigMap;
    }

    public List<String> getExcludeColumns(EntryWrapper entryWrapper) {
        return getConsumerConfig(entryWrapper).map(DbConsumerConfig::getExcludeColumns).orElse(Collections.emptyList());
    }

    public List<String> getIncludeColumns(EntryWrapper entryWrapper) {
        return getConsumerConfig(entryWrapper).map(DbConsumerConfig::getIncludeColumns).orElse(Collections.emptyList());
    }

    public List<String> getIgnoreChangeColumns(EntryWrapper entryWrapper) {
        return getConsumerConfig(entryWrapper).map(DbConsumerConfig::getIgnoreChangeColumns).orElse(Collections.emptyList());
    }

    public Map<String, String> getColumnMap(EntryWrapper entryWrapper) {
        return getConsumerConfig(entryWrapper).map(DbConsumerConfig::getColumnMap).orElse(Collections.emptyMap());
    }

    public Map<String, String> getDefaultValues(EntryWrapper entryWrapper) {
        return getConsumerConfig(entryWrapper).map(DbConsumerConfig::getDefaultValues).orElse(Collections.emptyMap());
    }

    public String getSchemaName(EntryWrapper entryWrapper) {
        return getConsumerConfig(entryWrapper).map(DbConsumerConfig::getSchemaName).orElse(entryWrapper.getSchemaName());
    }

    public String getTableName(EntryWrapper entryWrapper) {
        return getConsumerConfig(entryWrapper).map(DbConsumerConfig::getTableName).orElse(entryWrapper.getTableName());
    }

    public Optional<DbConsumerConfig> getConsumerConfig(EntryWrapper entryWrapper) {
        return Optional.ofNullable(consumerConfigMap)
                .map(map -> map.get(entryWrapper.getSchemaName()))
                .map(map -> map.get(entryWrapper.getTableName()));
    }

}
