package com.fanxuankai.canal.rocketmq.consumer;

import com.fanxuankai.canal.mq.core.config.CanalMqConfiguration;
import com.fanxuankai.canal.mq.core.consumer.DeleteFunction;
import org.apache.rocketmq.spring.core.RocketMQTemplate;

/**
 * 删除事件消费者
 *
 * @author fanxuankai
 */
public class DeleteConsumer extends AbstractConsumer implements DeleteFunction {
    public DeleteConsumer(CanalMqConfiguration canalMqConfiguration, RocketMQTemplate template) {
        super(canalMqConfiguration, template);
    }
}
