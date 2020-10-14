package com.fanxuankai.canal.mqbroker.rabbit.config;

import com.fanxuankai.boot.mqbroker.consume.EventListenerRegistry;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.InitializingBean;

import javax.annotation.Resource;

/**
 * @author fanxuankai
 */
public class CanalMqBrokerRabbitAutoConfiguration implements InitializingBean {

    @Resource
    private SimpleMessageListenerContainer container;
    @Resource
    private AmqpAdmin amqpAdmin;

    @Override
    public void afterPropertiesSet() {
        container.setQueues(EventListenerRegistry.getAllListenerMetadata()
                .stream()
                .map(o -> new Queue(o.getTopic()))
                .peek(amqpAdmin::declareQueue)
                .toArray(Queue[]::new));
    }

}
