package com.fanxuankai.canal.rocketmq.consumer;

import com.fanxuankai.canal.mq.core.config.CanalMqConfiguration;
import com.fanxuankai.canal.mq.core.consumer.AbstractMqConsumer;
import com.fanxuankai.canal.mq.core.model.MessageInfo;
import org.apache.rocketmq.spring.core.RocketMQTemplate;

/**
 * @author fanxuankai
 */
public abstract class AbstractConsumer extends AbstractMqConsumer<MessageInfo> {

    private final RocketMQTemplate template;

    public AbstractConsumer(CanalMqConfiguration canalMqConfiguration, RocketMQTemplate template) {
        super(canalMqConfiguration);
        this.template = template;
    }

    @Override
    public void accept(MessageInfo messageInfo) {
        messageInfo.getMessages().forEach(s -> template.convertAndSend(messageInfo.getTopic(), s));
    }

}
