package com.fanxuankai.canal.core.config;

import com.fanxuankai.canal.core.ConsumerConfigFactory;
import com.fanxuankai.canal.core.EntryConsumerFactory;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author fanxuankai
 */
public class CanalWorkConfiguration {
    private CanalConfiguration canalConfiguration;
    private ConsumerConfigFactory consumerConfigFactory;
    private EntryConsumerFactory entryConsumerFactory;
    private RedisTemplate<String, Object> redisTemplate;
    private ThreadPoolExecutor threadPoolExecutor;

    public CanalConfiguration getCanalConfiguration() {
        return canalConfiguration;
    }

    public void setCanalConfiguration(CanalConfiguration canalConfiguration) {
        this.canalConfiguration = canalConfiguration;
    }

    public ConsumerConfigFactory getConsumerConfigFactory() {
        return consumerConfigFactory;
    }

    public void setConsumerConfigFactory(ConsumerConfigFactory consumerConfigFactory) {
        this.consumerConfigFactory = consumerConfigFactory;
    }

    public EntryConsumerFactory getEntryConsumerFactory() {
        return entryConsumerFactory;
    }

    public void setEntryConsumerFactory(EntryConsumerFactory entryConsumerFactory) {
        this.entryConsumerFactory = entryConsumerFactory;
    }

    public RedisTemplate<String, Object> getRedisTemplate() {
        return redisTemplate;
    }

    public void setRedisTemplate(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public ThreadPoolExecutor getThreadPoolExecutor() {
        return threadPoolExecutor;
    }

    public void setThreadPoolExecutor(ThreadPoolExecutor threadPoolExecutor) {
        this.threadPoolExecutor = threadPoolExecutor;
    }
}
