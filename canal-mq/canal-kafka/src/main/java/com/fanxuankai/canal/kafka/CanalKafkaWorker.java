package com.fanxuankai.canal.kafka;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.fanxuankai.canal.core.CanalWorker;
import com.fanxuankai.canal.core.ConsumerConfigFactory;
import com.fanxuankai.canal.core.EntryConsumerFactory;
import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.core.config.CanalWorkConfiguration;
import com.fanxuankai.canal.kafka.consumer.DeleteConsumer;
import com.fanxuankai.canal.kafka.consumer.InsertConsumer;
import com.fanxuankai.canal.kafka.consumer.UpdateConsumer;
import com.fanxuankai.canal.mq.core.config.CanalMqConfiguration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.lang.Nullable;

import java.util.Optional;

/**
 * @author fanxuankai
 */
public class CanalKafkaWorker extends CanalWorker {

    public CanalKafkaWorker(CanalWorkConfiguration canalWorkConfiguration) {
        super(canalWorkConfiguration);
    }

    public static CanalKafkaWorker newCanalWorker(CanalConfiguration canalConfiguration,
                                                  @Nullable CanalMqConfiguration canalMqConfiguration,
                                                  KafkaTemplate<String, String> kafkaTemplate) {
        ConsumerConfigFactory consumerConfigFactory = new ConsumerConfigFactory();
        canalMqConfiguration = Optional.ofNullable(canalMqConfiguration)
                .orElse(new CanalMqConfiguration());
        canalMqConfiguration.getConsumerConfigMap().forEach((schema, consumerConfigMap) ->
                consumerConfigMap.forEach((table, consumerConfig) ->
                        consumerConfigFactory.put(schema, table, consumerConfig)));
        EntryConsumerFactory entryConsumerFactory = new EntryConsumerFactory();
        entryConsumerFactory.put(EventType.INSERT, new InsertConsumer(canalMqConfiguration, kafkaTemplate));
        entryConsumerFactory.put(EventType.UPDATE, new UpdateConsumer(canalMqConfiguration, kafkaTemplate));
        entryConsumerFactory.put(EventType.DELETE, new DeleteConsumer(canalMqConfiguration, kafkaTemplate));
        return new CanalKafkaWorker(new CanalWorkConfiguration()
                .setCanalConfiguration(canalConfiguration)
                .setConsumerConfigFactory(consumerConfigFactory)
                .setEntryConsumerFactory(entryConsumerFactory));
    }

}
