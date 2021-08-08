package com.fanxuankai.canal.test;

import com.fanxuankai.canal.redis.util.RedisTemplates;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * @author fanxuankai
 */
public class RedisTemplateUtils {
    public static RedisTemplate<String, Object> newRedisTemplate() {
        return RedisTemplates.newRedisTemplate(new JedisConnectionFactory());
    }
}
