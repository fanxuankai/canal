package com.fanxuankai.canal.rabbitmq.consumer;

import com.fanxuankai.canal.mq.core.config.CanalMqConfiguration;
import com.fanxuankai.canal.mq.core.consumer.UpdateFunction;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * 更新事件消费者
 *
 * @author fanxuankai
 */
public class UpdateConsumer extends AbstractConsumer implements UpdateFunction {

    public UpdateConsumer(CanalMqConfiguration canalMqConfiguration, RabbitTemplate rabbitTemplate,
                          AmqpAdmin amqpAdmin, Exchange exchange) {
        super(canalMqConfiguration, rabbitTemplate, amqpAdmin, exchange);
    }

}
