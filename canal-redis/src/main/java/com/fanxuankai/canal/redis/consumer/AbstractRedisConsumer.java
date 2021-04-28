package com.fanxuankai.canal.redis.consumer;

import com.fanxuankai.canal.core.EntryConsumer;
import com.fanxuankai.canal.core.config.ConsumerConfig;
import com.fanxuankai.canal.core.config.ConsumerConfigSupplier;
import com.fanxuankai.canal.core.model.EntryWrapper;
import com.fanxuankai.canal.core.util.RedisKey;
import com.fanxuankai.canal.redis.config.CanalRedisConfiguration;
import com.fanxuankai.canal.redis.util.RedisTemplates;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.StringUtils;

/**
 * Redis 抽象消费者
 *
 * @author fanxuankai
 */
public abstract class AbstractRedisConsumer<R> implements EntryConsumer<R>, ConsumerConfigSupplier {

    protected CanalRedisConfiguration canalRedisConfiguration;
    protected RedisTemplate<String, Object> redisTemplate;

    public AbstractRedisConsumer(CanalRedisConfiguration canalRedisConfiguration,
                                 RedisConnectionFactory redisConnectionFactory) {
        this.canalRedisConfiguration = canalRedisConfiguration;
        this.redisTemplate = RedisTemplates.newRedisTemplate(redisConnectionFactory);
    }

    @Override
    public ConsumerConfig getConsumerConfig(EntryWrapper entryWrapper) {
        return canalRedisConfiguration.getConsumerConfig(entryWrapper).orElse(null);
    }

    /**
     * key
     *
     * @param entryWrapper 数据
     * @return prefix.schema.table
     */
    protected String keyOf(EntryWrapper entryWrapper) {
        String key = canalRedisConfiguration.getKey(entryWrapper);
        if (StringUtils.hasText(key)) {
            return key;
        }
        return RedisKey.of(entryWrapper.getSchemaName(), entryWrapper.getTableName());
    }

    /**
     * key
     *
     * @param entryWrapper 数据
     * @param suffix       后缀
     * @return prefix.schema.table.suffix
     */
    protected String keyOf(EntryWrapper entryWrapper, String suffix) {
        String key = canalRedisConfiguration.getKey(entryWrapper);
        if (StringUtils.hasText(key)) {
            return RedisKey.withSuffix(key, suffix);
        }
        return RedisKey.of(entryWrapper.getSchemaName(), entryWrapper.getTableName(), suffix);
    }
}
