package com.fanxuankai.canal.kafka.consumer;

import com.fanxuankai.canal.mq.core.config.CanalMqConfiguration;
import com.fanxuankai.canal.mq.core.consumer.UpdateFunction;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * 更新事件消费者
 *
 * @author fanxuankai
 */
public class UpdateConsumer extends AbstractConsumer implements UpdateFunction {

    public UpdateConsumer(CanalMqConfiguration canalMqConfiguration, KafkaTemplate<String, String> kafkaTemplate) {
        super(canalMqConfiguration, kafkaTemplate);
    }
}
