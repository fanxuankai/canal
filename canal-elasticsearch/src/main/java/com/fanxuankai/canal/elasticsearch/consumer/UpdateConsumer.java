package com.fanxuankai.canal.elasticsearch.consumer;

import com.alibaba.fastjson.JSON;
import com.fanxuankai.canal.core.model.EntryWrapper;
import com.fanxuankai.canal.core.util.CommonUtils;
import com.fanxuankai.canal.core.util.DomainConverter;
import com.fanxuankai.canal.elasticsearch.*;
import com.fanxuankai.canal.elasticsearch.config.CanalElasticsearchConfiguration;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
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
public class UpdateConsumer extends AbstractEsConsumer<List<Object>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateConsumer.class);

    public UpdateConsumer(CanalElasticsearchConfiguration canalElasticsearchConfiguration,
                          IndexDefinitionManager indexDefinitionManager,
                          ElasticsearchRestTemplate elasticsearchRestTemplate) {
        super(canalElasticsearchConfiguration, indexDefinitionManager, elasticsearchRestTemplate);
    }

    static void updateByQuery(ElasticsearchRestTemplate template, UpdateByQueryParam updateByQueryParam) {
        RestHighLevelClient client = template.getClient();
        //参数为索引名，可以不指定，可以一个，可以多个
        UpdateByQueryRequest request = new UpdateByQueryRequest(updateByQueryParam.getIndexDefinition().getIndexName());
        // 更新时版本冲突
        request.setConflicts("proceed");
        // 设置查询条件，第一个参数是字段名，第二个参数是字段的值
        request.setQuery(updateByQueryParam.getUpdateByQuery().getQueryBuilder());
        String code = updateByQueryParam.getUpdateByQuery().getData()
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
            client.updateByQuery(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            LOGGER.error("update by query 异常", e);
        }
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
        try {
            List<UpdateQuery> updateQueries = objects.stream().filter(o -> o instanceof UpdateQuery)
                    .map(o -> (UpdateQuery) o)
                    .collect(Collectors.toList());
            if (!updateQueries.isEmpty()) {
                elasticsearchRestTemplate.bulkUpdate(updateQueries);
            }
        } catch (Exception e) {
            LOGGER.debug(e.getLocalizedMessage());
        }
        objects.stream().filter(o -> o instanceof UpdateByQueryParam)
                .map(o -> (UpdateByQueryParam) o)
                .forEach(updateByQueryParam -> UpdateConsumer.updateByQuery(elasticsearchRestTemplate,
                        updateByQueryParam));
    }

    private List<Object> queries(EntryWrapper entryWrapper, IndexDefinition indexDefinition) {
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
                        return Collections.singletonList(((OneToOneDocumentFunction<Object, Object>) function).applyForUpdate(before, after));
                    } else if (function instanceof ManyToOneDocumentFunction) {
                        return Collections.singletonList(((ManyToOneDocumentFunction<Object, Object>) function).applyForUpdate(before, after));
                    } else if (function instanceof ManyToManyDocumentFunction) {
                        return ((ManyToManyDocumentFunction<Object, Object>) function).applyForUpdate(before, after);
                    } else if (function instanceof OneToManyDocumentFunction) {
                        UpdateByQuery updateByQuery =
                                ((OneToManyDocumentFunction<Object, Object>) function).applyForUpdate(before, after);
                        UpdateByQueryParam updateByQueryParam = new UpdateByQueryParam();
                        updateByQueryParam.setUpdateByQuery(updateByQuery);
                        updateByQueryParam.setIndexDefinition(indexDefinition);
                        return Collections.singletonList(updateByQueryParam);
                    }
                    return Collections.emptyList();
                })
                .flatMap(Collection::stream)
                .filter(Objects::nonNull)
                .map(o -> {
                    if (o instanceof UpdateByQueryParam) {
                        return o;
                    }
                    UpdateQuery query = new UpdateQuery();
                    query.setId(getId(o));
                    query.setClazz(o.getClass());
                    query.setUpdateRequest(new UpdateRequest()
                            .doc(JSON.toJSONString(o), XContentType.JSON)
                            .docAsUpsert(function instanceof MasterDocumentFunction)
                    );
                    return query;
                })
                .collect(Collectors.toList());
    }

}
