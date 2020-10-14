package com.fanxuankai.canal.rocketmq.consumer;

import com.fanxuankai.canal.mq.core.config.CanalMqConfiguration;
import com.fanxuankai.canal.mq.core.consumer.InsertFunction;
import org.apache.rocketmq.spring.core.RocketMQTemplate;

/**
 * 新增事件消费者
 *
 * @author fanxuankai
 */
public class InsertConsumer extends AbstractConsumer implements InsertFunction {
    public InsertConsumer(CanalMqConfiguration canalMqConfiguration, RocketMQTemplate template) {
        super(canalMqConfiguration, template);
    }
}
