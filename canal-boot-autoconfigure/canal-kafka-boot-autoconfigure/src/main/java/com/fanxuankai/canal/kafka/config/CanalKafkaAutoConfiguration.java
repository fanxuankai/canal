package com.fanxuankai.canal.kafka.config;

import com.fanxuankai.canal.kafka.CanalKafkaWorker;
import com.fanxuankai.canal.mq.config.CanalMqProperties;
import com.fanxuankai.canal.mq.core.listener.ConsumerHelper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;

import java.util.ArrayList;
import java.util.List;

/**
 * @author fanxuankai
 */
public class CanalKafkaAutoConfiguration {

    @Bean
    @ConditionalOnProperty(prefix = CanalMqProperties.PREFIX, name = "enabled", havingValue = "true")
    public CanalKafkaWorker canalKafkaWorker(CanalMqProperties canalMqProperties,
                                             KafkaTemplate<String, String> kafkaTemplate) {
        return CanalKafkaWorker.newCanalWorker(canalMqProperties.getConfiguration(), canalMqProperties, kafkaTemplate);
    }

    @Bean
    public KafkaMessageListenerContainer<String, String> messageListenerContainer(ConsumerFactory<String, Object> consumerFactory,
                                                                                  ConsumerHelper consumerHelper) {
        List<String> topics = new ArrayList<>();
        consumerHelper.accept((canalListenerDefinition, s) -> topics.add(s));
        ContainerProperties properties = new ContainerProperties(topics.toArray(new String[0]));
        properties.setGroupId("canal-mq-group");
        properties.setMessageListener((MessageListener<String, String>) data ->
                consumerHelper.consume(data.topic(), data.value()));
        return new KafkaMessageListenerContainer<>(consumerFactory, properties);
    }
}
