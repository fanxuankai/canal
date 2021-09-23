package com.fanxuankai.canal.elasticsearch;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.fanxuankai.canal.core.CanalWorker;
import com.fanxuankai.canal.core.ConsumerConfigFactory;
import com.fanxuankai.canal.core.EntryConsumerFactory;
import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.core.config.CanalWorkConfiguration;
import com.fanxuankai.canal.elasticsearch.config.CanalElasticsearchConfiguration;
import com.fanxuankai.canal.elasticsearch.consumer.DeleteConsumer;
import com.fanxuankai.canal.elasticsearch.consumer.EraseConsumer;
import com.fanxuankai.canal.elasticsearch.consumer.InsertConsumer;
import com.fanxuankai.canal.elasticsearch.consumer.UpdateConsumer;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.lang.Nullable;

import java.util.Optional;

/**
 * @author fanxuankai
 */
public class CanalElasticsearchWorker extends CanalWorker {

    public CanalElasticsearchWorker(CanalWorkConfiguration canalWorkConfiguration) {
        super(canalWorkConfiguration);
    }

    public static CanalElasticsearchWorker newCanalWorker(CanalConfiguration canalConfiguration,
                                                          @Nullable CanalElasticsearchConfiguration canalElasticsearchConfiguration,
                                                          IndexDefinitionManager indexDefinitionManager,
                                                          ElasticsearchRestTemplate elasticsearchRestTemplate,
                                                          RestHighLevelClient restHighLevelClient) {
        ConsumerConfigFactory consumerConfigFactory = new ConsumerConfigFactory();
        canalElasticsearchConfiguration = Optional.ofNullable(canalElasticsearchConfiguration)
                .orElse(new CanalElasticsearchConfiguration());
        canalElasticsearchConfiguration.getConsumerConfigMap().forEach((schema, consumerConfigMap) ->
                consumerConfigMap.forEach((table, redisConsumerConfig) ->
                        consumerConfigFactory.put(schema, table, redisConsumerConfig)));
        EntryConsumerFactory entryConsumerFactory = new EntryConsumerFactory();
        entryConsumerFactory.put(EventType.INSERT, new InsertConsumer(canalElasticsearchConfiguration,
                indexDefinitionManager, elasticsearchRestTemplate, restHighLevelClient));
        entryConsumerFactory.put(EventType.UPDATE, new UpdateConsumer(canalElasticsearchConfiguration,
                indexDefinitionManager, elasticsearchRestTemplate, restHighLevelClient));
        entryConsumerFactory.put(EventType.DELETE, new DeleteConsumer(canalElasticsearchConfiguration,
                indexDefinitionManager, elasticsearchRestTemplate, restHighLevelClient));
        entryConsumerFactory.put(EventType.ERASE, new EraseConsumer(canalElasticsearchConfiguration,
                indexDefinitionManager, elasticsearchRestTemplate, restHighLevelClient));
        CanalWorkConfiguration canalWorkConfiguration = new CanalWorkConfiguration();
        canalWorkConfiguration.setCanalConfiguration(canalConfiguration);
        canalWorkConfiguration.setConsumerConfigFactory(consumerConfigFactory);
        canalWorkConfiguration.setEntryConsumerFactory(entryConsumerFactory);
        return new CanalElasticsearchWorker(canalWorkConfiguration);
    }

}
