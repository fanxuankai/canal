package com.fanxuankai.canal.redis.config;

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
public class CanalRedisConfiguration {

    private Map<String, Map<String, RedisConsumerConfig>> consumerConfigMap = Collections.emptyMap();

    public String getKey(EntryWrapper entryWrapper) {
        return getConsumerConfig(entryWrapper).map(RedisConsumerConfig::getKey).orElse(null);
    }

    public boolean isIdAsHashKey(EntryWrapper entryWrapper) {
        return getConsumerConfig(entryWrapper).map(RedisConsumerConfig::isIdAsHashKey).orElse(true);
    }

    public List<String> getUniqueKeys(EntryWrapper entryWrapper) {
        return getConsumerConfig(entryWrapper).map(RedisConsumerConfig::getUniqueKeys).orElse(Collections.emptyList());
    }

    public List<List<String>> getCombineKeys(EntryWrapper entryWrapper) {
        return getConsumerConfig(entryWrapper).map(RedisConsumerConfig::getCombineKeys).orElse(Collections.emptyList());
    }

    public Optional<RedisConsumerConfig> getConsumerConfig(EntryWrapper entryWrapper) {
        return Optional.ofNullable(consumerConfigMap)
                .map(map -> map.get(entryWrapper.getSchemaName()))
                .map(map -> map.get(entryWrapper.getTableName()));
    }

}
