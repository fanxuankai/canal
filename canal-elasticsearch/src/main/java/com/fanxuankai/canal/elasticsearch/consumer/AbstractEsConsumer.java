package com.fanxuankai.canal.elasticsearch.consumer;

import com.fanxuankai.canal.core.EntryConsumer;
import com.fanxuankai.canal.core.config.ConsumerConfig;
import com.fanxuankai.canal.core.config.ConsumerConfigSupplier;
import com.fanxuankai.canal.core.model.EntryWrapper;
import com.fanxuankai.canal.elasticsearch.IndexDefinitionManager;
import com.fanxuankai.canal.elasticsearch.config.CanalElasticsearchConfiguration;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;

import java.lang.reflect.Field;
import java.util.Objects;

/**
 * Es 抽象消费者
 *
 * @author fanxuankai
 */
public abstract class AbstractEsConsumer<R> implements EntryConsumer<R>, ConsumerConfigSupplier {

    protected CanalElasticsearchConfiguration canalElasticsearchConfiguration;
    protected IndexDefinitionManager indexDefinitionManager;
    protected ElasticsearchRestTemplate elasticsearchRestTemplate;

    public AbstractEsConsumer(CanalElasticsearchConfiguration canalElasticsearchConfiguration,
                              IndexDefinitionManager indexDefinitionManager,
                              ElasticsearchRestTemplate elasticsearchRestTemplate) {
        this.canalElasticsearchConfiguration = canalElasticsearchConfiguration;
        this.indexDefinitionManager = indexDefinitionManager;
        this.elasticsearchRestTemplate = elasticsearchRestTemplate;
    }

    @Override
    public ConsumerConfig getConsumerConfig(EntryWrapper entryWrapper) {
        return canalElasticsearchConfiguration.getConsumerConfig(entryWrapper).orElse(null);
    }

    protected String getId(Object object) {
        Class<?> aClass = object.getClass();
        for (Field field : aClass.getDeclaredFields()) {
            field.setAccessible(true);
            if (Objects.equals(field.getName(), "id") || field.getAnnotation(Id.class) != null) {
                try {
                    return field.get(object).toString();
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        throw new RuntimeException("该类无法找到 id 字段: " + aClass.getName());
    }

}
