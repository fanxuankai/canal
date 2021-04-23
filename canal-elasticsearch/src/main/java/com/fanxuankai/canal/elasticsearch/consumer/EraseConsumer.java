package com.fanxuankai.canal.elasticsearch.consumer;

import com.fanxuankai.canal.core.model.EntryWrapper;
import com.fanxuankai.canal.elasticsearch.IndexDefinition;
import com.fanxuankai.canal.elasticsearch.IndexDefinitionManager;
import com.fanxuankai.canal.elasticsearch.config.CanalElasticsearchConfiguration;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 删表事件消费者
 *
 * @author fanxuankai
 */
public class EraseConsumer extends AbstractEsConsumer<List<Class<?>>> {

    public EraseConsumer(CanalElasticsearchConfiguration canalElasticsearchConfiguration,
                         IndexDefinitionManager indexDefinitionManager,
                         ElasticsearchRestTemplate elasticsearchRestTemplate, RestHighLevelClient restHighLevelClient) {
        super(canalElasticsearchConfiguration, indexDefinitionManager, elasticsearchRestTemplate, restHighLevelClient);
    }

    @Override
    public List<Class<?>> apply(EntryWrapper entryWrapper) {
        return indexDefinitionManager.getIndexDefinitions(entryWrapper.getSchemaName(), entryWrapper.getTableName())
                .stream()
                .map(IndexDefinition::getDocumentClass)
                .collect(Collectors.toList());
    }

    @Override
    public boolean filterable() {
        return false;
    }

    @Override
    public void accept(List<Class<?>> documentClasses) {
        documentClasses.forEach(o -> elasticsearchRestTemplate.indexOps(o).delete());
    }

}
