package com.fanxuankai.canal.xxlmq.consumer;

import com.fanxuankai.canal.mq.core.config.CanalMqConfiguration;
import com.fanxuankai.canal.mq.core.consumer.DeleteFunction;

/**
 * 删除事件消费者
 *
 * @author fanxuankai
 */
public class DeleteConsumer extends AbstractConsumer implements DeleteFunction {

    public DeleteConsumer(CanalMqConfiguration canalMqConfiguration) {
        super(canalMqConfiguration);
    }

}
