package com.fanxuankai.canal.kafka.consumer;

import com.fanxuankai.canal.mq.core.config.CanalMqConfiguration;
import com.fanxuankai.canal.mq.core.consumer.InsertFunction;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * 新增事件消费者
 *
 * @author fanxuankai
 */
public class InsertConsumer extends AbstractConsumer implements InsertFunction {

    public InsertConsumer(CanalMqConfiguration canalMqConfiguration, KafkaTemplate<String, String> kafkaTemplate) {
        super(canalMqConfiguration, kafkaTemplate);
    }
}
