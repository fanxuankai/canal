package com.fanxuankai.canal.rocketmq.consumer;

import com.fanxuankai.canal.mq.core.config.CanalMqConfiguration;
import com.fanxuankai.canal.mq.core.consumer.UpdateFunction;
import org.apache.rocketmq.spring.core.RocketMQTemplate;

/**
 * 更新事件消费者
 *
 * @author fanxuankai
 */
public class UpdateConsumer extends AbstractConsumer implements UpdateFunction {
    public UpdateConsumer(CanalMqConfiguration canalMqConfiguration, RocketMQTemplate template) {
        super(canalMqConfiguration, template);
    }
}
