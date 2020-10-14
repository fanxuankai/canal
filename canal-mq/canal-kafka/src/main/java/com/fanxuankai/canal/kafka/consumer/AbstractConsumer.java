package com.fanxuankai.canal.kafka.consumer;

import com.fanxuankai.canal.mq.core.config.CanalMqConfiguration;
import com.fanxuankai.canal.mq.core.consumer.AbstractMqConsumer;
import com.fanxuankai.canal.mq.core.model.MessageInfo;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * @author fanxuankai
 */
public abstract class AbstractConsumer extends AbstractMqConsumer<MessageInfo> {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public AbstractConsumer(CanalMqConfiguration canalMqConfiguration, KafkaTemplate<String, String> kafkaTemplate) {
        super(canalMqConfiguration);
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void accept(MessageInfo messageInfo) {
        messageInfo.getMessages().forEach(s -> kafkaTemplate.send(messageInfo.getTopic(), s));
    }

}
