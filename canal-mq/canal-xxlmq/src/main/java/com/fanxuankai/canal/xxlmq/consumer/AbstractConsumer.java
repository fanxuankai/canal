package com.fanxuankai.canal.xxlmq.consumer;

import com.fanxuankai.canal.mq.core.config.CanalMqConfiguration;
import com.fanxuankai.canal.mq.core.consumer.AbstractMqConsumer;
import com.fanxuankai.canal.mq.core.model.MessageInfo;
import com.xxl.mq.client.message.XxlMqMessage;
import com.xxl.mq.client.producer.XxlMqProducer;

/**
 * @author fanxuankai
 */
public abstract class AbstractConsumer extends AbstractMqConsumer<MessageInfo> {

    public AbstractConsumer(CanalMqConfiguration canalMqConfiguration) {
        super(canalMqConfiguration);
    }

    @Override
    public void accept(MessageInfo messageInfo) {
        messageInfo.getMessages().forEach(s -> {
            XxlMqMessage mqMessage = new XxlMqMessage(messageInfo.getTopic(), s);
            mqMessage.setGroup(messageInfo.getGroup());
            XxlMqProducer.produce(mqMessage);
        });
    }

}
