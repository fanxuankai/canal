package com.fanxuankai.canal.xxlmq.consumer;

import com.fanxuankai.canal.mq.core.config.CanalMqConfiguration;
import com.fanxuankai.canal.mq.core.consumer.UpdateFunction;

/**
 * 更新事件消费者
 *
 * @author fanxuankai
 */
public class UpdateConsumer extends AbstractConsumer implements UpdateFunction {

    public UpdateConsumer(CanalMqConfiguration canalMqConfiguration) {
        super(canalMqConfiguration);
    }

}
