package com.fanxuankai.canal.xxlmq;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.fanxuankai.canal.core.CanalWorker;
import com.fanxuankai.canal.core.ConsumerConfigFactory;
import com.fanxuankai.canal.core.EntryConsumerFactory;
import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.core.config.CanalWorkConfiguration;
import com.fanxuankai.canal.mq.core.config.CanalMqConfiguration;
import com.fanxuankai.canal.xxlmq.consumer.DeleteConsumer;
import com.fanxuankai.canal.xxlmq.consumer.InsertConsumer;
import com.fanxuankai.canal.xxlmq.consumer.UpdateConsumer;
import org.springframework.lang.Nullable;

import java.util.Optional;

/**
 * @author fanxuankai
 */
public class CanalXxlMqWorker extends CanalWorker {

    public CanalXxlMqWorker(CanalWorkConfiguration canalWorkConfiguration) {
        super(canalWorkConfiguration);
    }

    public static CanalXxlMqWorker newCanalWorker(CanalConfiguration canalConfiguration,
                                                  @Nullable CanalMqConfiguration canalMqConfiguration) {
        ConsumerConfigFactory consumerConfigFactory = new ConsumerConfigFactory();
        canalMqConfiguration = Optional.ofNullable(canalMqConfiguration)
                .orElse(new CanalMqConfiguration());
        canalMqConfiguration.getConsumerConfigMap().forEach((schema, consumerConfigMap) ->
                consumerConfigMap.forEach((table, consumerConfig) ->
                        consumerConfigFactory.put(schema, table, consumerConfig)));
        EntryConsumerFactory entryConsumerFactory = new EntryConsumerFactory();
        entryConsumerFactory.put(EventType.INSERT, new InsertConsumer(canalMqConfiguration));
        entryConsumerFactory.put(EventType.UPDATE, new UpdateConsumer(canalMqConfiguration));
        entryConsumerFactory.put(EventType.DELETE, new DeleteConsumer(canalMqConfiguration));
        CanalWorkConfiguration canalWorkConfiguration = new CanalWorkConfiguration();
        canalWorkConfiguration.setCanalConfiguration(canalConfiguration);
        canalWorkConfiguration.setConsumerConfigFactory(consumerConfigFactory);
        canalWorkConfiguration.setEntryConsumerFactory(entryConsumerFactory);
        return new CanalXxlMqWorker(canalWorkConfiguration);
    }

}
