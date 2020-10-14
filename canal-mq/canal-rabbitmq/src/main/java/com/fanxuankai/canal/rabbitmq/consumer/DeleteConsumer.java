package com.fanxuankai.canal.rabbitmq.consumer;

import com.fanxuankai.canal.mq.core.config.CanalMqConfiguration;
import com.fanxuankai.canal.mq.core.consumer.DeleteFunction;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * 删除事件消费者
 *
 * @author fanxuankai
 */
public class DeleteConsumer extends AbstractConsumer implements DeleteFunction {

    public DeleteConsumer(CanalMqConfiguration canalMqConfiguration, RabbitTemplate rabbitTemplate,
                          AmqpAdmin amqpAdmin, Exchange exchange) {
        super(canalMqConfiguration, rabbitTemplate, amqpAdmin, exchange);
    }

}
