package com.fanxuankai.canal.elasticsearch.consumer;

import cn.hutool.core.util.ArrayUtil;
import com.alibaba.fastjson.JSON;
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
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.UpdateQuery;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 新增事件消费者
 *
 * @author fanxuankai
 */
public class InsertConsumer extends AbstractEsConsumer<List<QueryData>> {

    public InsertConsumer(CanalElasticsearchConfiguration canalElasticsearchConfiguration,
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
    public void accept(List<QueryData> list) {
        if (CollectionUtils.isEmpty(list)) {
            return;
        }
        List<QueryData> indexQueryDataList = list.stream()
                .filter(o -> o.getQuery() instanceof IndexQuery)
                .collect(Collectors.toList());
        if (!indexQueryDataList.isEmpty()) {
            String[] indexNames = ArrayUtil.toArray(indexQueryDataList.stream()
                    .map(QueryData::getIndexName)
                    .distinct()
                    .collect(Collectors.toList()), String.class);
            elasticsearchRestTemplate.bulkIndex(indexQueryDataList.stream()
                    .map(o -> (IndexQuery) o.getQuery())
                    .collect(Collectors.toList()), IndexCoordinates.of(indexNames));
        }
        UpdateConsumer.update(list, elasticsearchRestTemplate, restHighLevelClient);
    }

    private List<QueryData> queries(EntryWrapper entryWrapper, IndexDefinition indexDefinition) {
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
                        return Collections.singletonList(new UpdateByQueryParam(indexDefinition,
                                ((OneToManyDocumentFunction<Object, Object>) function).applyForInsert(insert)));
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
                    QueryData queryData = new QueryData();
                    queryData.setIndexName(indexDefinition.getIndexName());
                    Object query;
                    if (function instanceof MasterDocumentFunction) {
                        IndexQuery indexQuery = new IndexQuery();
                        indexQuery.setObject(o);
                        query = indexQuery;
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
}
