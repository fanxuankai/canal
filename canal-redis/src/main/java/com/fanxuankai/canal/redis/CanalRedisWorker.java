package com.fanxuankai.canal.redis;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.core.CanalWorker;
import com.fanxuankai.canal.core.ConsumerConfigFactory;
import com.fanxuankai.canal.core.EntryConsumerFactory;
import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.core.config.CanalWorkConfiguration;
import com.fanxuankai.canal.redis.config.CanalRedisConfiguration;
import com.fanxuankai.canal.redis.consumer.DeleteConsumer;
import com.fanxuankai.canal.redis.consumer.EraseConsumer;
import com.fanxuankai.canal.redis.consumer.InsertOrUpdateConsumer;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.lang.Nullable;

import java.util.Optional;

/**
 * @author fanxuankai
 */
public class CanalRedisWorker extends CanalWorker {
    public CanalRedisWorker(CanalWorkConfiguration canalWorkConfiguration) {
        super(canalWorkConfiguration);
    }

    public static CanalRedisWorker newCanalWorker(CanalConfiguration canalConfiguration,
                                                  @Nullable CanalRedisConfiguration canalRedisConfiguration,
                                                  RedisTemplate<String, Object> redisTemplate) {
        ConsumerConfigFactory consumerConfigFactory = new ConsumerConfigFactory();
        canalRedisConfiguration = Optional.ofNullable(canalRedisConfiguration)
                .orElse(new CanalRedisConfiguration());
        canalRedisConfiguration.getConsumerConfigMap().forEach((schema, consumerConfigMap) ->
                consumerConfigMap.forEach((table, consumerConfig) ->
                        consumerConfigFactory.put(schema, table, consumerConfig)));
        EntryConsumerFactory entryConsumerFactory = new EntryConsumerFactory();
        RedisConnectionFactory redisConnectionFactory = redisTemplate.getRequiredConnectionFactory();
        InsertOrUpdateConsumer insertOrUpdateConsumer =
                new InsertOrUpdateConsumer(canalRedisConfiguration, redisConnectionFactory);
        entryConsumerFactory.put(CanalEntry.EventType.INSERT, insertOrUpdateConsumer);
        entryConsumerFactory.put(CanalEntry.EventType.UPDATE, insertOrUpdateConsumer);
        entryConsumerFactory.put(CanalEntry.EventType.DELETE, new DeleteConsumer(canalRedisConfiguration,
                redisConnectionFactory));
        entryConsumerFactory.put(CanalEntry.EventType.ERASE, new EraseConsumer(canalRedisConfiguration,
                redisConnectionFactory));
        CanalWorkConfiguration canalWorkConfiguration = new CanalWorkConfiguration();
        canalWorkConfiguration.setCanalConfiguration(canalConfiguration);
        canalWorkConfiguration.setConsumerConfigFactory(consumerConfigFactory);
        canalWorkConfiguration.setEntryConsumerFactory(entryConsumerFactory);
        canalWorkConfiguration.setRedisTemplate(redisTemplate);
        return new CanalRedisWorker(canalWorkConfiguration);
    }
}