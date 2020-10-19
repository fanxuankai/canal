package com.fanxuankai.canal.test;

import com.fanxuankai.canal.core.CanalWorker;
import com.fanxuankai.canal.core.config.CanalConfiguration;
import com.fanxuankai.canal.elasticsearch.CanalElasticsearchWorker;
import com.fanxuankai.canal.elasticsearch.IndexDefinitionManager;
import com.fanxuankai.canal.test.domain.User;
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
        CanalWorker canalWorker = CanalElasticsearchWorker.newCanalWorker(new CanalConfiguration()
                        .setInstance("canalEsExample")
                        .setShowEventLog(true)
                        .setShowEntryLog(true),
                null, indexDefinitionManager,
                new ElasticsearchRestTemplate(RestClients.create(ClientConfiguration.localhost()).rest()));
        canalWorker.getCanalWorkConfiguration()
                .setRedisTemplate(RedisTemplates.newRedisTemplate());
        canalWorker.start();
    }
}