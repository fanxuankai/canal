package com.fanxuankai.canal.redis.consumer;

import com.fanxuankai.canal.core.model.EntryWrapper;
import com.fanxuankai.canal.redis.config.CanalRedisConfiguration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.util.CollectionUtils;

import java.util.Collection;
import java.util.Collections;

/**
 * 删表事件消费者
 *
 * @author fanxuankai
 */
public class EraseConsumer extends AbstractRedisConsumer<Collection<String>> {

    public EraseConsumer(CanalRedisConfiguration canalRedisConfiguration,
                         RedisConnectionFactory redisConnectionFactory) {
        super(canalRedisConfiguration, redisConnectionFactory);
    }

    @Override
    public Collection<String> apply(EntryWrapper entryWrapper) {
        Collection<String> keys = redisTemplate.keys(keyOf(entryWrapper) + "*");
        if (CollectionUtils.isEmpty(keys)) {
            return Collections.emptySet();
        }
        return keys;
    }

    @Override
    public boolean filterable() {
        return false;
    }

    @Override
    public void accept(Collection<String> keys) {
        redisTemplate.delete(keys);
    }
}
