package com.fanxuankai.canal.redis.config;

import com.fanxuankai.canal.core.config.ConsumerConfig;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * @author fanxuankai
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class RedisConsumerConfig extends ConsumerConfig {
    /**
     * 默认为 schema.table
     */
    private String key;
    /**
     * id 是否作为 hashKey
     */
    private boolean idAsHashKey = true;
    /**
     * hash key 增加 uniqueKey 后缀, 作为 hash 的集合名, 以 uniqueKey 的值作为 hash 的 field
     */
    private List<String> uniqueKeys;
    /**
     * hash key 增加 combineKeys 后缀, 作为 hash 的集合名, 以 combineKeys 的值作为 hash 的 field
     */
    private List<List<String>> combineKeys;
}