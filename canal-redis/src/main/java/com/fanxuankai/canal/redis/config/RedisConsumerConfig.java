package com.fanxuankai.canal.redis.config;

import com.fanxuankai.canal.core.config.ConsumerConfig;

import java.util.List;

/**
 * @author fanxuankai
 */
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

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public boolean isIdAsHashKey() {
        return idAsHashKey;
    }

    public void setIdAsHashKey(boolean idAsHashKey) {
        this.idAsHashKey = idAsHashKey;
    }

    public List<String> getUniqueKeys() {
        return uniqueKeys;
    }

    public void setUniqueKeys(List<String> uniqueKeys) {
        this.uniqueKeys = uniqueKeys;
    }

    public List<List<String>> getCombineKeys() {
        return combineKeys;
    }

    public void setCombineKeys(List<List<String>> combineKeys) {
        this.combineKeys = combineKeys;
    }
}