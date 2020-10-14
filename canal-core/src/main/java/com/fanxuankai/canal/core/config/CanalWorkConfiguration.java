package com.fanxuankai.canal.core.config;

import com.fanxuankai.canal.core.ConsumerConfigFactory;
import com.fanxuankai.canal.core.EntryConsumerFactory;
import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author fanxuankai
 */
@Data
@Accessors(chain = true)
public class CanalWorkConfiguration {
    private CanalConfiguration canalConfiguration;
    private ConsumerConfigFactory consumerConfigFactory;
    private EntryConsumerFactory entryConsumerFactory;
    private RedisTemplate<String, Object> redisTemplate;
    private ThreadPoolExecutor threadPoolExecutor;
}
