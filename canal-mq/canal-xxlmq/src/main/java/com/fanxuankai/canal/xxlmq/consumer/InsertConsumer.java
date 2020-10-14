package com.fanxuankai.canal.xxlmq.consumer;

import com.fanxuankai.canal.mq.core.config.CanalMqConfiguration;
import com.fanxuankai.canal.mq.core.consumer.InsertFunction;

/**
 * 新增事件消费者
 *
 * @author fanxuankai
 */
public class InsertConsumer extends AbstractConsumer implements InsertFunction {

    public InsertConsumer(CanalMqConfiguration canalMqConfiguration) {
        super(canalMqConfiguration);
    }

}
