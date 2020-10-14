package com.fanxuankai.canal.mqbroker.consumer;

import com.fanxuankai.boot.mqbroker.model.Event;
import com.fanxuankai.boot.mqbroker.produce.EventPublisher;
import com.fanxuankai.canal.mq.core.config.CanalMqConfiguration;
import com.fanxuankai.canal.mq.core.consumer.AbstractMqConsumer;

import java.util.List;

/**
 * MQ 抽象消费者
 *
 * @author fanxuankai
 */
public abstract class AbstractConsumer extends AbstractMqConsumer<List<Event<String>>> {

    private final EventPublisher<String> eventPublisher;

    public AbstractConsumer(CanalMqConfiguration canalMqConfiguration, EventPublisher<String> eventPublisher) {
        super(canalMqConfiguration);
        this.eventPublisher = eventPublisher;
    }

    @Override
    public void accept(List<Event<String>> events) {
        eventPublisher.publish(events);
    }

}
