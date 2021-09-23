package com.fanxuankai.canal.elasticsearch.consumer;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.canal.core.model.EntryWrapper;
import com.fanxuankai.canal.core.util.CommonUtils;
import com.fanxuankai.canal.core.util.DomainConverter;
import com.fanxuankai.canal.elasticsearch.*;
import com.fanxuankai.canal.elasticsearch.config.CanalElasticsearchConfiguration;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.document.Document;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.UpdateQuery;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 删除事件消费者
 *
 * @author fanxuankai
 */
public class DeleteConsumer extends AbstractEsConsumer<List<QueryData>> {

    public DeleteConsumer(CanalElasticsearchConfiguration canalElasticsearchConfiguration,
                          IndexDefinitionManager indexDefinitionManager,
                          ElasticsearchRestTemplate elasticsearchRestTemplate,
                          RestHighLevelClient restHighLevelClient) {
        super(canalElasticsearchConfiguration, indexDefinitionManager, elasticsearchRestTemplate, restHighLevelClient);
    }

    @Override
    public List<QueryData> apply(EntryWrapper entryWrapper) {
        return indexDefinitionManager.getIndexDefinitions(entryWrapper.getSchemaName(), entryWrapper.getTableName())
                .stream()
                .map(indexDefinition -> queries(entryWrapper, indexDefinition))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    @Override
    public boolean filterable() {
        return false;
    }

    @Override
    public void accept(List<QueryData> list) {
        if (CollectionUtils.isEmpty(list)) {
            return;
        }
        list.stream().filter(o -> o.getQuery() instanceof DeleteObject)
                .forEach(queryData -> {
                    DeleteObject deleteObject = (DeleteObject) queryData.getQuery();
                    elasticsearchRestTemplate.delete(deleteObject.getId(),
                            IndexCoordinates.of(queryData.getIndexName()));
                });
        UpdateConsumer.update(list, elasticsearchRestTemplate, restHighLevelClient);
    }

    private List<QueryData> queries(EntryWrapper entryWrapper, IndexDefinition indexDefinition) {
        DocumentFunction<Object, Object> function = indexDefinition.getDocumentFunction();
        return entryWrapper.getAllRowDataList()
                .stream()
                .map(CanalEntry.RowData::getBeforeColumnsList)
                .map(columns -> DomainConverter.of(CommonUtils.toJsonString(columns),
                        indexDefinition.getEntityClass()))
                .map(delete -> {
                    if (function instanceof MasterDocumentFunction) {
                        String id = ((MasterDocumentFunction<Object, Object>) function).applyForDelete(delete);
                        DeleteObject deleteObject = new DeleteObject();
                        deleteObject.setId(id);
                        return Collections.singletonList(deleteObject);
                    } else if (function instanceof OneToOneDocumentFunction) {
                        return Collections.singletonList(((OneToOneDocumentFunction<Object, Object>) function).applyForDelete(delete));
                    } else if (function instanceof OneToManyDocumentFunction) {
                        return Collections.singletonList(((OneToManyDocumentFunction<Object, Object>) function).applyForDelete(delete));
                    } else if (function instanceof ManyToOneDocumentFunction) {
                        return Collections.singletonList(((ManyToOneDocumentFunction<Object, Object>) function).applyForDelete(delete));
                    } else if (function instanceof ManyToManyDocumentFunction) {
                        return ((ManyToManyDocumentFunction<Object, Object>) function).applyForDelete(delete);
                    }
                    return Collections.emptyList();
                })
                .flatMap(Collection::stream)
                .filter(Objects::nonNull)
                .map(o -> {
                    QueryData queryData = new QueryData();
                    queryData.setIndexName(indexDefinition.getIndexName());
                    Object query;
                    if (o instanceof DeleteObject || o instanceof UpdateByQuery) {
                        query = o;
                    } else {
                        query = UpdateQuery.builder(getId(o))
                                .withDocument(Document.parse(JSON.toJSONString(o)))
                                .build();
                    }
                    queryData.setQuery(query);
                    return queryData;
                })
                .collect(Collectors.toList());
    }

    private static final class DeleteObject {
        private String id;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }
    }
}
