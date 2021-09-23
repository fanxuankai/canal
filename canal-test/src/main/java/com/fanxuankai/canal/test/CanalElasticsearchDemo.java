package com.fanxuankai.canal.test;

import com.fanxuankai.canal.core.CanalWorker;
import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.elasticsearch.CanalElasticsearchWorker;
import com.fanxuankai.canal.elasticsearch.IndexDefinitionManager;
import com.fanxuankai.canal.test.domain.User;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.RestClients;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;

import java.util.Collections;

/**
 * @author fanxuankai
 */
public class CanalElasticsearchDemo {
    public static void main(String[] args) {
        IndexDefinitionManager indexDefinitionManager =
                IndexDefinitionManager.from(Collections.singleton(User.class));
        CanalConfiguration canalConfiguration = new CanalConfiguration();
        canalConfiguration.setInstance("canalEsExample");
        canalConfiguration.setFilter("canal_client_example.t_user");
        canalConfiguration.setShowEventLog(true);
        canalConfiguration.setShowEntryLog(true);
        canalConfiguration.setBatchSize(10000);
        RestHighLevelClient rest = RestClients.create(ClientConfiguration.localhost()).rest();
        CanalWorker canalWorker = CanalElasticsearchWorker.newCanalWorker(canalConfiguration,
                null, indexDefinitionManager, new ElasticsearchRestTemplate(rest), rest);
        canalWorker.getCanalWorkConfiguration()
                .setRedisTemplate(RedisTemplateUtils.newRedisTemplate());
        canalWorker.start();
    }
}