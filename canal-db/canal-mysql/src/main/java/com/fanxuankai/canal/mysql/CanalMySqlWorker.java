package com.fanxuankai.canal.mysql;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.core.CanalWorker;
import com.fanxuankai.canal.core.ConsumerConfigFactory;
import com.fanxuankai.canal.core.EntryConsumerFactory;
import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.core.config.CanalWorkConfiguration;
import com.fanxuankai.canal.db.core.config.CanalDbConfiguration;
import com.fanxuankai.canal.mysql.consumer.DeleteConsumer;
import com.fanxuankai.canal.mysql.consumer.InsertConsumer;
import com.fanxuankai.canal.mysql.consumer.UpdateConsumer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.lang.Nullable;

import java.util.Optional;

/**
 * @author fanxuankai
 */
public class CanalMySqlWorker extends CanalWorker {
    public CanalMySqlWorker(CanalWorkConfiguration canalWorkConfiguration) {
        super(canalWorkConfiguration);
    }

    public static CanalMySqlWorker newCanalWorker(CanalConfiguration canalConfiguration,
                                                  @Nullable CanalDbConfiguration canalDbConfiguration,
                                                  JdbcTemplate jdbcTemplate) {
        ConsumerConfigFactory consumerConfigFactory = new ConsumerConfigFactory();
        canalDbConfiguration = Optional.ofNullable(canalDbConfiguration)
                .orElse(new CanalDbConfiguration());
        canalDbConfiguration.getConsumerConfigMap().forEach((schema, consumerConfigMap) ->
                consumerConfigMap.forEach((table, consumerConfig) ->
                        consumerConfigFactory.put(schema, table, consumerConfig)));
        EntryConsumerFactory entryConsumerFactory = new EntryConsumerFactory();
        entryConsumerFactory.put(CanalEntry.EventType.INSERT, new InsertConsumer(jdbcTemplate, canalDbConfiguration));
        entryConsumerFactory.put(CanalEntry.EventType.UPDATE, new UpdateConsumer(jdbcTemplate, canalDbConfiguration));
        entryConsumerFactory.put(CanalEntry.EventType.DELETE, new DeleteConsumer(jdbcTemplate, canalDbConfiguration));
        CanalWorkConfiguration canalWorkConfiguration = new CanalWorkConfiguration();
        canalWorkConfiguration.setCanalConfiguration(canalConfiguration);
        canalWorkConfiguration.setConsumerConfigFactory(consumerConfigFactory);
        canalWorkConfiguration.setEntryConsumerFactory(entryConsumerFactory);
        return new CanalMySqlWorker(canalWorkConfiguration);
    }

}
