package com.fanxuankai.canal.elasticsearch.consumer;

import cn.hutool.core.util.ArrayUtil;
import com.alibaba.fastjson.JSON;
import com.fanxuankai.canal.core.model.EntryWrapper;
import com.fanxuankai.canal.core.util.CommonUtils;
import com.fanxuankai.canal.core.util.DomainConverter;
import com.fanxuankai.canal.elasticsearch.*;
import com.fanxuankai.canal.elasticsearch.config.CanalElasticsearchConfiguration;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.document.Document;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.UpdateQuery;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 修改事件消费者
 *
 * @author fanxuankai
 */
public class UpdateConsumer extends AbstractEsConsumer<List<QueryData>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateConsumer.class);

    public UpdateConsumer(CanalElasticsearchConfiguration canalElasticsearchConfiguration,
                          IndexDefinitionManager indexDefinitionManager,
                          ElasticsearchRestTemplate elasticsearchRestTemplate,
                          RestHighLevelClient restHighLevelClient) {
        super(canalElasticsearchConfiguration, indexDefinitionManager, elasticsearchRestTemplate, restHighLevelClient);
    }

    static void update(List<QueryData> list, ElasticsearchRestTemplate template,
                       RestHighLevelClient restHighLevelClient) {
        if (CollectionUtils.isEmpty(list)) {
            return;
        }
        List<QueryData> updateQueryDataList = list.stream()
                .filter(o -> o.getQuery() instanceof UpdateQuery)
                .collect(Collectors.toList());
        if (!updateQueryDataList.isEmpty()) {
            try {
                String[] indexNames = ArrayUtil.toArray(updateQueryDataList.stream()
                        .map(QueryData::getIndexName)
                        .distinct()
                        .collect(Collectors.toList()), String.class);
                template.bulkUpdate(updateQueryDataList.stream()
                        .map(o -> (UpdateQuery) o.getQuery())
                        .collect(Collectors.toList()), IndexCoordinates.of(indexNames));
            } catch (Exception e) {
                LOGGER.error(e.getLocalizedMessage());
            }
        }
        list.stream().filter(o -> o.getQuery() instanceof UpdateByQuery)
                .forEach(queryData -> {
                    UpdateByQuery updateByQuery = (UpdateByQuery) queryData.getQuery();
                    //参数为索引名，可以不指定，可以一个，可以多个
                    UpdateByQueryRequest request =
                            new UpdateByQueryRequest(queryData.getIndexName());
                    // 更新时版本冲突
                    request.setConflicts("proceed");
                    // 设置查询条件，第一个参数是字段名，第二个参数是字段的值
                    request.setQuery(updateByQuery.getQueryBuilder());
                    String code = updateByQuery.getData()
                            .entrySet()
                            .stream()
                            .map(entry -> {
                                Object value = entry.getValue();
                                String valueString;
                                if (value instanceof CharSequence
                                        || value instanceof Character) {
                                    valueString = "'" + value + "'";
                                } else {
                                    valueString = String.valueOf(value);
                                }
                                return "ctx._source." + entry.getKey() + " = " + valueString;
                            })
                            .collect(Collectors.joining(";", "", ""));
                    request.setScript(new Script(ScriptType.INLINE, "painless",
                            code, Collections.emptyMap()));
                    // 并行
                    request.setSlices(2);
                    // 使用滚动参数来控制“搜索上下文”存活的时间
                    request.setScroll(TimeValue.timeValueMinutes(10));
                    // 可选参数
                    // 超时
                    request.setTimeout(TimeValue.timeValueMinutes(2));
                    // 刷新索引
                    request.setRefresh(true);
                    try {
                        restHighLevelClient.updateByQuery(request, RequestOptions.DEFAULT);
                    } catch (IOException e) {
                        LOGGER.error("update by query 异常", e);
                    }
                });
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
        update(list, elasticsearchRestTemplate, restHighLevelClient);
    }

    private List<QueryData> queries(EntryWrapper entryWrapper, IndexDefinition indexDefinition) {
        DocumentFunction<Object, Object> function = indexDefinition.getDocumentFunction();
        return entryWrapper.getAllRowDataList()
                .stream()
                .map(rowData -> {
                    Object before = DomainConverter.of(CommonUtils.toJsonString(rowData.getBeforeColumnsList()),
                            indexDefinition.getEntityClass());
                    Object after = DomainConverter.of(CommonUtils.toJsonString(rowData.getAfterColumnsList()),
                            indexDefinition.getEntityClass());
                    if (function instanceof MasterDocumentFunction) {
                        return Collections.singletonList(((MasterDocumentFunction<Object, Object>) function).applyForUpdate(before, after));
                    } else if (function instanceof OneToOneDocumentFunction) {
                        return ((OneToOneDocumentFunction<Object, Object>) function).applyForUpdate(before, after);
                    } else if (function instanceof OneToManyDocumentFunction) {
                        return Collections.singletonList(((OneToManyDocumentFunction<Object, Object>) function).applyForUpdate(before, after));
                    } else if (function instanceof ManyToOneDocumentFunction) {
                        return Collections.singletonList(((ManyToOneDocumentFunction<Object, Object>) function).applyForUpdate(before, after));
                    } else if (function instanceof ManyToManyDocumentFunction) {
                        return ((ManyToManyDocumentFunction<Object, Object>) function).applyForUpdate(before, after);
                    }
                    return Collections.emptyList();
                })
                .flatMap(Collection::stream)
                .filter(Objects::nonNull)
                .map(o -> {
                    QueryData queryData = new QueryData();
                    queryData.setIndexName(indexDefinition.getIndexName());
                    Object query;
                    if (o instanceof UpdateByQuery) {
                        query = o;
                    } else {
                        query = UpdateQuery.builder(getId(o))
                                .withDocument(Document.parse(JSON.toJSONString(o)))
                                .withDocAsUpsert(function instanceof MasterDocumentFunction)
                                .build();
                    }
                    queryData.setQuery(query);
                    return queryData;
                })
                .collect(Collectors.toList());
    }

}
