package com.fanxuankai.canal.rabbitmq.config;

import com.fanxuankai.canal.mq.config.CanalMqProperties;
import com.fanxuankai.canal.mq.core.listener.ConsumerHelper;
import com.fanxuankai.canal.rabbitmq.CanalRabbitMqWorker;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * @author fanxuankai
 */
public class CanalRabbitMqAutoConfiguration {

    @Bean
    @ConditionalOnProperty(prefix = CanalMqProperties.PREFIX, name = "enabled", havingValue = "true")
    public CanalRabbitMqWorker canalRabbitMqWorker(CanalMqProperties canalMqProperties,
                                                   RabbitTemplate rabbitTemplate,
                                                   AmqpAdmin amqpAdmin) {
        return CanalRabbitMqWorker.newCanalWorker(canalMqProperties.getConfiguration(), canalMqProperties,
                rabbitTemplate, amqpAdmin);
    }

    @Bean
    public MessageListenerContainer messageListenerContainer(ConnectionFactory connectionFactory,
                                                             AmqpAdmin amqpAdmin,
                                                             ConsumerHelper consumerHelper) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
        // 监听的队列
        consumerHelper.accept((canalListenerDefinition, s) -> {
            Queue queue = new Queue(s);
            amqpAdmin.declareQueue(queue);
            container.addQueues(queue);
        });
        // 设置监听器
        container.setMessageListener(message -> {
            String msg = new String(message.getBody());
            MessageProperties messageProperties = message.getMessageProperties();
            consumerHelper.consume(messageProperties.getConsumerQueue(), msg);
        });
        return container;
    }
}
