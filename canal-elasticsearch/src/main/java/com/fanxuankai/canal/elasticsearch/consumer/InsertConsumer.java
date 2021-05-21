package com.fanxuankai.canal.elasticsearch.consumer;

import com.alibaba.fastjson.JSON;
import com.fanxuankai.canal.core.model.EntryWrapper;
import com.fanxuankai.canal.core.util.CommonUtils;
import com.fanxuankai.canal.core.util.DomainConverter;
import com.fanxuankai.canal.elasticsearch.*;
import com.fanxuankai.canal.elasticsearch.config.CanalElasticsearchConfiguration;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.UpdateQuery;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 新增事件消费者
 *
 * @author fanxuankai
 */
public class InsertConsumer extends AbstractEsConsumer<List<Object>> {

    public InsertConsumer(CanalElasticsearchConfiguration canalElasticsearchConfiguration,
                          IndexDefinitionManager indexDefinitionManager,
                          ElasticsearchRestTemplate elasticsearchRestTemplate) {
        super(canalElasticsearchConfiguration, indexDefinitionManager, elasticsearchRestTemplate);
    }

    @Override
    public List<Object> apply(EntryWrapper entryWrapper) {
        return indexDefinitionManager.getIndexDefinitions(entryWrapper.getSchemaName(), entryWrapper.getTableName())
                .stream()
                .map(indexDefinition -> queries(entryWrapper, indexDefinition))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    @Override
    public void accept(List<Object> objects) {
        if (CollectionUtils.isEmpty(objects)) {
            return;
        }
        List<IndexQuery> indexQueries = objects.stream()
                .filter(o -> o instanceof IndexQuery)
                .map(o -> (IndexQuery) o)
                .collect(Collectors.toList());
        if (!indexQueries.isEmpty()) {
            elasticsearchRestTemplate.bulkIndex(indexQueries);
        }
        UpdateConsumer.update(objects, elasticsearchRestTemplate);
    }

    private List<Object> queries(EntryWrapper entryWrapper, IndexDefinition indexDefinition) {
        DocumentFunction<Object, Object> function = indexDefinition.getDocumentFunction();
        return entryWrapper.getAllRowDataList()
                .stream()
                .map(rowData -> DomainConverter.of(CommonUtils.toJsonString(rowData.getAfterColumnsList()),
                        indexDefinition.getEntityClass()))
                .map(insert -> {
                    if (function instanceof MasterDocumentFunction) {
                        return Collections.singletonList(((MasterDocumentFunction<Object, Object>) function).applyForInsert(insert));
                    } else if (function instanceof OneToOneDocumentFunction) {
                        return Collections.singletonList(((OneToOneDocumentFunction<Object, Object>) function).applyForInsert(insert));
                    } else if (function instanceof OneToManyDocumentFunction) {
                        return Optional.ofNullable(((OneToManyDocumentFunction<Object, Object>) function).applyForInsert(insert))
                                .map(o -> Collections.singletonList(new UpdateByQueryParam(indexDefinition, o)))
                                .orElse(Collections.emptyList());
                    } else if (function instanceof ManyToOneDocumentFunction) {
                        return Collections.singletonList(((ManyToOneDocumentFunction<Object, Object>) function).applyForInsert(insert));
                    } else if (function instanceof ManyToManyDocumentFunction) {
                        return ((ManyToManyDocumentFunction<Object, Object>) function).applyForInsert(insert);
                    }
                    return Collections.emptyList();
                })
                .flatMap(Collection::stream)
                .filter(Objects::nonNull)
                .map(o -> {
                    if (function instanceof MasterDocumentFunction) {
                        IndexQuery query = new IndexQuery();
                        query.setObject(o);
                        return query;
                    }
                    if (o instanceof UpdateByQueryParam) {
                        return o;
                    }
                    UpdateQuery query = new UpdateQuery();
                    query.setId(getId(o));
                    query.setClazz(o.getClass());
                    query.setUpdateRequest(new UpdateRequest()
                            .doc(JSON.toJSONString(o), XContentType.JSON));
                    return query;
                })
                .collect(Collectors.toList());
    }
}
