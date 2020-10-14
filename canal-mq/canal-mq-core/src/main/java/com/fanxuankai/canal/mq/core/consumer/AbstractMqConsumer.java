package com.fanxuankai.canal.mq.core.consumer;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.core.EntryConsumer;
import com.fanxuankai.canal.core.config.ConsumerConfig;
import com.fanxuankai.canal.core.config.ConsumerConfigSupplier;
import com.fanxuankai.canal.core.model.EntryWrapper;
import com.fanxuankai.canal.core.util.Topic;
import com.fanxuankai.canal.mq.core.config.CanalMqConfiguration;
import org.springframework.util.StringUtils;

import java.util.Optional;

/**
 * MQ 抽象消费者
 *
 * @author fanxuankai
 */
public abstract class AbstractMqConsumer<R> implements EntryConsumer<R>, TopicSupplier, GroupSupplier,
        ConsumerConfigSupplier {
    protected CanalMqConfiguration canalMqConfiguration;

    public AbstractMqConsumer(CanalMqConfiguration canalMqConfiguration) {
        this.canalMqConfiguration = canalMqConfiguration;
    }

    @Override
    public String getTopic(EntryWrapper entryWrapper) {
        String topic = canalMqConfiguration.getTopic(entryWrapper);
        CanalEntry.EventType eventType = entryWrapper.getEventType();
        if (StringUtils.hasText(topic)) {
            return Topic.custom(topic, eventType.name());
        }
        return Topic.of(entryWrapper.getSchemaName(), entryWrapper.getTableName(), eventType.name());
    }

    @Override
    public String getGroup(EntryWrapper entryWrapper) {
        return Optional.ofNullable(canalMqConfiguration.getGroup(entryWrapper))
                .filter(StringUtils::hasText)
                .orElse(canalMqConfiguration.getGlobalGroup());
    }

    @Override
    public ConsumerConfig getConsumerConfig(EntryWrapper entryWrapper) {
        return canalMqConfiguration.getConsumerConfig(entryWrapper).orElse(null);
    }

}
