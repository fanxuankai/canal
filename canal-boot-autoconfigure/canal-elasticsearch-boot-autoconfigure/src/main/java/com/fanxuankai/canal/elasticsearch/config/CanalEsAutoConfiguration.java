package com.fanxuankai.canal.elasticsearch.config;

import com.fanxuankai.canal.elasticsearch.CanalElasticsearchWorker;
import com.fanxuankai.canal.elasticsearch.IndexDefinitionManager;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;

/**
 * @author fanxuankai
 */
@ConditionalOnProperty(prefix = CanalEsProperties.PREFIX, name = "enabled", havingValue = "true")
@EnableConfigurationProperties(CanalEsProperties.class)
public class CanalEsAutoConfiguration {

    private final IndexDefinitionManager indexDefinitionManager;

    public CanalEsAutoConfiguration(IndexDefinitionManager indexDefinitionManager) {
        this.indexDefinitionManager = indexDefinitionManager;
    }

    @Bean
    public CanalElasticsearchWorker canalElasticsearchWorker(CanalEsProperties canalEsProperties,
                                                             ElasticsearchRestTemplate elasticsearchRestTemplate) {
        return CanalElasticsearchWorker.newCanalWorker(canalEsProperties.getConfiguration(), canalEsProperties,
                indexDefinitionManager, elasticsearchRestTemplate);
    }
}
