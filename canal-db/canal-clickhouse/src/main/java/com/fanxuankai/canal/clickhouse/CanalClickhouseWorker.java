package com.fanxuankai.canal.clickhouse;

import com.fanxuankai.canal.clickhouse.consumer.DeleteConsumer;
import com.fanxuankai.canal.clickhouse.consumer.InsertConsumer;
import com.fanxuankai.canal.clickhouse.consumer.UpdateConsumer;
import com.fanxuankai.canal.core.CanalWorker;
import com.fanxuankai.canal.core.ConsumerConfigFactory;
import com.fanxuankai.canal.core.EntryConsumerFactory;
import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.core.config.CanalWorkConfiguration;
import com.fanxuankai.canal.db.core.config.CanalDbConfiguration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.lang.Nullable;

import java.util.Optional;

import static com.alibaba.otter.canal.protocol.CanalEntry.EventType.*;

/**
 * @author fanxuankai
 */
public class CanalClickhouseWorker extends CanalWorker {
    public CanalClickhouseWorker(CanalWorkConfiguration canalWorkConfiguration) {
        super(canalWorkConfiguration);
    }

    public static CanalClickhouseWorker newCanalWorker(CanalConfiguration canalConfiguration,
                                                       @Nullable CanalDbConfiguration canalDbConfiguration,
                                                       JdbcTemplate jdbcTemplate) {
        ConsumerConfigFactory consumerConfigFactory = new ConsumerConfigFactory();
        canalDbConfiguration = Optional.ofNullable(canalDbConfiguration)
                .orElse(new CanalDbConfiguration());
        canalDbConfiguration.getConsumerConfigMap().forEach((schema, consumerConfigMap) ->
                consumerConfigMap.forEach((table, consumerConfig) ->
                        consumerConfigFactory.put(schema, table, consumerConfig)));
        EntryConsumerFactory entryConsumerFactory = new EntryConsumerFactory();
        entryConsumerFactory.put(INSERT, new InsertConsumer(jdbcTemplate, canalDbConfiguration));
        entryConsumerFactory.put(UPDATE, new UpdateConsumer(jdbcTemplate, canalDbConfiguration));
        entryConsumerFactory.put(DELETE, new DeleteConsumer(jdbcTemplate, canalDbConfiguration));
        CanalWorkConfiguration canalWorkConfiguration = new CanalWorkConfiguration();
        canalWorkConfiguration.setCanalConfiguration(canalConfiguration);
        canalWorkConfiguration.setConsumerConfigFactory(consumerConfigFactory);
        canalWorkConfiguration.setEntryConsumerFactory(entryConsumerFactory);
        return new CanalClickhouseWorker(canalWorkConfiguration);
    }
}
