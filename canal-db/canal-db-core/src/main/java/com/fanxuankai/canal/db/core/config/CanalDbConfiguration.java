package com.fanxuankai.canal.db.core.config;

import com.fanxuankai.canal.core.model.EntryWrapper;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author fanxuankai
 */
@Data
@Accessors(chain = true)
public class CanalDbConfiguration {

    /**
     * 是否开启 canal 服务
     */
    private Boolean enabled = Boolean.TRUE;

    private Map<String, Map<String, DbConsumerConfig>> consumerConfigMap = Collections.emptyMap();

    public List<String> getExcludeColumns(EntryWrapper entryWrapper) {
        return getConsumerConfig(entryWrapper).map(DbConsumerConfig::getExcludeColumns).orElse(null);
    }

    public List<String> getIncludeColumns(EntryWrapper entryWrapper) {
        return getConsumerConfig(entryWrapper).map(DbConsumerConfig::getIncludeColumns).orElse(null);
    }

    public Map<String, String> getColumnMap(EntryWrapper entryWrapper) {
        return getConsumerConfig(entryWrapper).map(DbConsumerConfig::getColumnMap).orElse(Collections.emptyMap());
    }

    public Map<String, String> getDefaultValues(EntryWrapper entryWrapper) {
        return getConsumerConfig(entryWrapper).map(DbConsumerConfig::getDefaultValues).orElse(Collections.emptyMap());
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
