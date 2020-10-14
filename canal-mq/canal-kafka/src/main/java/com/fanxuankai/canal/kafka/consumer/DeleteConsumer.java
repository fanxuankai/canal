package com.fanxuankai.canal.kafka.consumer;

import com.fanxuankai.canal.mq.core.config.CanalMqConfiguration;
import com.fanxuankai.canal.mq.core.consumer.DeleteFunction;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * 删除事件消费者
 *
 * @author fanxuankai
 */
public class DeleteConsumer extends AbstractConsumer implements DeleteFunction {

    public DeleteConsumer(CanalMqConfiguration canalMqConfiguration, KafkaTemplate<String, String> kafkaTemplate) {
        super(canalMqConfiguration, kafkaTemplate);
    }

}
