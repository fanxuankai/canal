package com.fanxuankai.canal.rocketmq;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.fanxuankai.canal.core.CanalWorker;
import com.fanxuankai.canal.core.ConsumerConfigFactory;
import com.fanxuankai.canal.core.EntryConsumerFactory;
import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.core.config.CanalWorkConfiguration;
import com.fanxuankai.canal.mq.core.config.CanalMqConfiguration;
import com.fanxuankai.canal.rocketmq.consumer.DeleteConsumer;
import com.fanxuankai.canal.rocketmq.consumer.InsertConsumer;
import com.fanxuankai.canal.rocketmq.consumer.UpdateConsumer;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.lang.Nullable;

import java.util.Optional;

/**
 * @author fanxuankai
 */
public class CanalRocketMqWorker extends CanalWorker {

    public CanalRocketMqWorker(CanalWorkConfiguration canalWorkConfiguration) {
        super(canalWorkConfiguration);
    }

    public static CanalRocketMqWorker newCanalWorker(CanalConfiguration canalConfiguration,
                                                     @Nullable CanalMqConfiguration canalMqConfiguration,
                                                     RocketMQTemplate rocketMqTemplate) {
        ConsumerConfigFactory consumerConfigFactory = new ConsumerConfigFactory();
        canalMqConfiguration = Optional.ofNullable(canalMqConfiguration)
                .orElse(new CanalMqConfiguration());
        canalMqConfiguration.getConsumerConfigMap().forEach((schema, consumerConfigMap) ->
                consumerConfigMap.forEach((table, consumerConfig) ->
                        consumerConfigFactory.put(schema, table, consumerConfig)));
        EntryConsumerFactory entryConsumerFactory = new EntryConsumerFactory();
        entryConsumerFactory.put(EventType.INSERT, new InsertConsumer(canalMqConfiguration, rocketMqTemplate));
        entryConsumerFactory.put(EventType.UPDATE, new UpdateConsumer(canalMqConfiguration, rocketMqTemplate));
        entryConsumerFactory.put(EventType.DELETE, new DeleteConsumer(canalMqConfiguration, rocketMqTemplate));
        return new CanalRocketMqWorker(new CanalWorkConfiguration()
                .setCanalConfiguration(canalConfiguration)
                .setConsumerConfigFactory(consumerConfigFactory)
                .setEntryConsumerFactory(entryConsumerFactory));
    }

}
