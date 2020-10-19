package com.fanxuankai.canal.elasticsearch.config;

import com.fanxuankai.canal.core.config.ConsumerConfig;
import com.fanxuankai.canal.core.model.EntryWrapper;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * @author fanxuankai
 */
@Data
@Accessors(chain = true)
public class CanalElasticsearchConfiguration {

    private Map<String, Map<String, ConsumerConfig>> consumerConfigMap = Collections.emptyMap();

    public Optional<ConsumerConfig> getConsumerConfig(EntryWrapper entryWrapper) {
        return Optional.ofNullable(consumerConfigMap)
                .map(map -> map.get(entryWrapper.getSchemaName()))
                .map(map -> map.get(entryWrapper.getTableName()));
    }

}
