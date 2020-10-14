package com.fanxuankai.canal.rabbitmq.consumer;

import com.fanxuankai.canal.mq.core.config.CanalMqConfiguration;
import com.fanxuankai.canal.mq.core.consumer.AbstractMqConsumer;
import com.fanxuankai.canal.mq.core.model.MessageInfo;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.util.HashSet;
import java.util.Set;

/**
 * @author fanxuankai
 */
public abstract class AbstractConsumer extends AbstractMqConsumer<MessageInfo> {
    private final RabbitTemplate rabbitTemplate;
    private final AmqpAdmin amqpAdmin;
    private final Exchange exchange;
    private final Set<String> queueCache = new HashSet<>();

    public AbstractConsumer(CanalMqConfiguration canalMqConfiguration, RabbitTemplate rabbitTemplate,
                            AmqpAdmin amqpAdmin, Exchange exchange) {
        super(canalMqConfiguration);
        this.rabbitTemplate = rabbitTemplate;
        this.amqpAdmin = amqpAdmin;
        this.exchange = exchange;
    }

    @Override
    public void accept(MessageInfo messageInfo) {
        if (!queueCache.contains(messageInfo.getTopic())) {
            Queue queue = new Queue(messageInfo.getTopic());
            amqpAdmin.declareQueue(queue);
            amqpAdmin.declareBinding(BindingBuilder.bind(queue).to(exchange).with(messageInfo.getTopic()).noargs());
            queueCache.add(messageInfo.getTopic());
        }
        messageInfo.getMessages().forEach(s ->
                rabbitTemplate.convertAndSend(exchange.getName(), messageInfo.getTopic(), s));
    }

}
